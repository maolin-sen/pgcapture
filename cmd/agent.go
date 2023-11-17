package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/replicase/pgcapture/pkg/cursor"
	"github.com/replicase/pgcapture/pkg/dblog"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/sink"
	"github.com/replicase/pgcapture/pkg/source"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	AgentListenAddr  string
	AgentRenice      int64
	ExportPrometheus bool
	PrometheusAddr   string
)

func init() {
	rootCmd.AddCommand(agent)
	agent.Flags().StringVarP(&AgentListenAddr, "ListenAddr", "", ":10000", "the tcp address for agent server to listen")
	agent.Flags().Int64VarP(&AgentRenice, "Renice", "", -10, "try renice the sink pg process")
	agent.Flags().BoolVarP(&ExportPrometheus, "ExportPrometheus", "", false, "export the prometheus metrics or not")
	agent.Flags().StringVarP(&PrometheusAddr, "PrometheusAddr", "", ":2112", "the tcp address for prometheus server to listen")
}

var agent = &cobra.Command{
	Use:   "agent",
	Short: "run as a agent accepting remote config",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		if ExportPrometheus {
			startPrometheusServer(PrometheusAddr)
		}

		logrus.WithFields(logrus.Fields{
			"AgentListenAddr": AgentListenAddr,
		}).Info("starting agent")

		agent := &Agent{}
		return serveGRPC(&pb.Agent_ServiceDesc, AgentListenAddr, agent, func() {
			if err := agent.cleanup(); err != nil {
				logrus.Errorf("agent failed to cleanup: %v", err)
			}
		})
	},
}

type Agent struct {
	pb.UnimplementedAgentServer

	mu         sync.Mutex
	params     *structpb.Struct
	dumper     *dblog.PGXSourceDumper
	pgSink     *sink.PGXSink
	pulsarSink *sink.PulsarSink
	pgSrc      *source.PGXSource
	sinkErr    error
	sourceErr  error
}

func (a *Agent) Configure(ctx context.Context, request *pb.AgentConfigRequest) (*pb.AgentConfigResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.params != nil {
		return a.report(a.params)
	}

	var params *structpb.Struct

	if params = request.GetParameters(); params == nil {
		return nil, errors.New("parameter is required")
	}

	if v, err := extract(params, "Command"); err != nil {
		return nil, err
	} else {
		switch v["Command"].GetStringValue() {
		case "pg2pulsar":
			return a.pg2pulsar(params)
		case "pulsar2pg":
			return a.pulsar2pg(params)
		case "status":
			return a.report(a.params)
		default:
			return nil, errors.New("'Command' should be one of [pg2pulsar|pulsar2pg|status]")
		}
	}
}

// Dump 根据请求下载指定快照数据
func (a *Agent) Dump(ctx context.Context, req *pb.AgentDumpRequest) (*pb.AgentDumpResponse, error) {
	var dumper *dblog.PGXSourceDumper
	a.mu.Lock()
	dumper = a.dumper
	a.mu.Unlock()

	if dumper == nil {
		return nil, status.Error(codes.Aborted, "dumper is not ready")
	}

	changes, err := dumper.LoadDump(req.MinLsn, req.Info)
	if err != nil {
		switch err {
		case dblog.ErrMissingTable:
			return nil, status.Error(codes.NotFound, err.Error())
		case dblog.ErrLSNMissing:
			return nil, status.Error(codes.Unavailable, err.Error())
		case dblog.ErrLSNFallBehind:
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, err
	}
	return &pb.AgentDumpResponse{Change: changes}, nil
}

// StreamDump 根据请求下载指定快照数据，并流式发送change
func (a *Agent) StreamDump(req *pb.AgentDumpRequest, server pb.Agent_StreamDumpServer) error {
	resp, err := a.Dump(server.Context(), req)
	if err != nil {
		return err
	}
	for _, change := range resp.Change {
		if err = server.Send(change); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) cleanup() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	defer func() {
		a.dumper = nil
		a.pgSink = nil
		a.pulsarSink = nil
		a.pgSrc = nil
	}()

	if a.dumper != nil {
		a.dumper.Stop()
	}

	var err error
	if a.pgSink != nil {
		a.pgSink.Stop()
		if err == nil {
			err = a.pgSink.Error()
		}
	}
	if a.pulsarSink != nil {
		a.pulsarSink.Stop()
		if err == nil {
			err = a.pulsarSink.Error()
		}
	}
	if a.pgSrc != nil {
		a.pgSrc.Stop()
		if err == nil {
			err = a.pgSrc.Error()
		}
	}
	return err
}

func (a *Agent) pg2pulsar(params *structpb.Struct) (*pb.AgentConfigResponse, error) {
	v, err := extract(params, "PGConnURL", "PGReplURL", "PulsarURL", "PulsarTopic", "DecodePlugin", "?StartLSN", "?PulsarTracker", "?PulsarTrackerInterval", "?PulsarTrackerReplicateState")
	if err != nil {
		return nil, err
	}

	pgSrc := &source.PGXSource{SetupConnStr: v["PGConnURL"].GetStringValue(), ReplConnStr: v["PGReplURL"].GetStringValue(), ReplSlot: trimSlot(v["PulsarTopic"].GetStringValue()), CreateSlot: true, CreatePublication: true, StartLSN: v["StartLSN"].GetStringValue(), DecodePlugin: v["DecodePlugin"].GetStringValue()}
	pulsarSink := &sink.PulsarSink{PulsarOption: pulsar.ClientOptions{URL: v["PulsarURL"].GetStringValue()}, PulsarTopic: v["PulsarTopic"].GetStringValue()}

	switch v["PulsarTracker"].GetStringValue() {
	case "pulsar", "":
		pulsarSink.SetupTracker = func(client pulsar.Client, topic string) (cursor.Tracker, error) {
			return cursor.NewPulsarTracker(client, topic)
		}
	case "pulsarSub":
		// set the default value to 1min
		commitInterval := time.Minute
		if val := v["PulsarTrackerInterval"].GetStringValue(); val != "" {
			var err error
			commitInterval, err = time.ParseDuration(val)
			if err != nil {
				return nil, fmt.Errorf("PulsarTrackerInterval should be a valid duration: %w", err)
			}
		}

		pulsarSink.SetupTracker = func(client pulsar.Client, topic string) (cursor.Tracker, error) {
			return cursor.NewPulsarSubscriptionTracker(client, topic, commitInterval, v["PulsarTrackerReplicateState"].GetBoolValue())
		}
	default:
		return nil, errors.New("PulsarTracker should be one of [pulsar|pulsarSub]")
	}

	a.pulsarSink = pulsarSink
	a.pgSrc = pgSrc

	logger := logrus.WithFields(logrus.Fields{
		"PulsarURL":   v["PulsarURL"].GetStringValue(),
		"PulsarTopic": v["PulsarTopic"].GetStringValue(),
	})
	logger.Info("start pg2pulsar")

	if err := a.sourceToSink(pgSrc, pulsarSink); err != nil {
		logger.Fatalf("sourceToSink err: %v", err)
		return nil, err
	}

	a.params = params
	return a.report(a.params)
}

func (a *Agent) pulsar2pg(params *structpb.Struct) (*pb.AgentConfigResponse, error) {
	v, err := extract(params, "PGConnURL", "PulsarURL", "PulsarTopic", "?PGLogPath", "?BatchTxSize")
	if err != nil {
		return nil, err
	}

	batchTXSize := v["BatchTxSize"].GetNumberValue()
	if batchTXSize == 0 {
		batchTXSize = 100
	} else if batchTXSize < 0 || batchTXSize != math.Trunc(batchTXSize) {
		return nil, errors.New("BatchTxSize should be a positive integer")
	}

	pgSink := &sink.PGXSink{ConnStr: v["PGConnURL"].GetStringValue(), SourceID: trimSlot(v["PulsarTopic"].GetStringValue()), Renice: AgentRenice, LogReader: nil, BatchTXSize: int(batchTXSize)}
	if path := v["PGLogPath"].GetStringValue(); path != "" {
		pgLog, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		pgSink.LogReader = pgLog
	}

	dumper, err := dblog.NewPGXSourceDumper(context.Background(), v["PGConnURL"].GetStringValue())
	if err != nil {
		return nil, err
	}

	a.dumper = dumper
	a.pgSink = pgSink

	logger := logrus.WithFields(logrus.Fields{
		"PulsarURL":   v["PulsarURL"].GetStringValue(),
		"PulsarTopic": v["PulsarTopic"].GetStringValue(),
		"PGLogPath":   v["PGLogPath"].GetStringValue(),
		"BatchTxSize": batchTXSize,
	})
	logger.Info("start pulsar2pg")

	pulsarSrc := &source.PulsarReaderSource{PulsarOption: pulsar.ClientOptions{URL: v["PulsarURL"].GetStringValue()}, PulsarTopic: v["PulsarTopic"].GetStringValue()}
	if err = a.sourceToSink(pulsarSrc, pgSink); err != nil {
		logger.Fatalf("sourceToSink error: %v", err)
		return nil, err
	}

	a.params = params
	return a.report(a.params)
}

func (a *Agent) sourceToSink(src source.Source, sk sink.Sinker) (err error) {
	lastCheckPoint, err := sk.Setup()
	if err != nil {
		return err
	}

	changes, err := src.Capture(lastCheckPoint)
	if err != nil {
		sk.Stop()
		return err
	}

	go func() {
		checkpoints := sk.Apply(changes)
		for cp := range checkpoints {
			src.Commit(cp)
		}
	}()
	go func() {
		check := func() bool {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.sinkErr = sk.Error()
			a.sourceErr = src.Error()
			if a.sinkErr != nil {
				a.params = nil
				a.pgSink = nil
				logrus.Errorf("sink error: %v", a.sinkErr)
			}
			if a.sourceErr != nil {
				a.params = nil
				a.pgSrc = nil
				a.pulsarSink = nil
				logrus.Errorf("source error: %v", a.sourceErr)
			}
			if a.dumper != nil && (a.sourceErr != nil || a.sinkErr != nil) {
				a.dumper.Stop()
				a.dumper = nil
			}
			return a.sinkErr == nil && a.sourceErr == nil
		}
		for check() {
			time.Sleep(time.Second)
		}
		sk.Stop()
		src.Stop()
	}()

	a.sinkErr = nil
	a.sourceErr = nil

	return nil
}

func (a *Agent) report(params *structpb.Struct) (*pb.AgentConfigResponse, error) {
	if a.sinkErr != nil || a.sourceErr != nil {
		return nil, fmt.Errorf("sinkErr: %v, sourceErr: %v", a.sinkErr, a.sourceErr)
	}
	if a.pgSink != nil {
		params.Fields["ReplicationLagMilliseconds"] = structpb.NewNumberValue(float64(a.pgSink.ReplicationLagMilliseconds()))
	} else if a.pgSrc != nil {
		params.Fields["SourceTxCounter"] = structpb.NewNumberValue(float64(a.pgSrc.TxCounter()))
	}
	return &pb.AgentConfigResponse{Report: params}, nil
}

func parseKey(k string) (parsed string, optional bool) {
	if strings.HasPrefix(k, "?") {
		return k[1:], true
	}
	return k, false
}

func extract(params *structpb.Struct, keys ...string) (map[string]*structpb.Value, error) {
	values := map[string]*structpb.Value{}
	for _, v := range keys {
		k, optional := parseKey(v)
		if fields := params.GetFields(); (fields == nil || fields[k] == nil || reflect.ValueOf(fields[k].AsInterface()).IsZero()) && !optional {
			return nil, fmt.Errorf("%s key is required in parameters", k)
		} else {
			values[k] = fields[k]
		}
	}
	return values, nil
}
