package dblog

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"net"
	"regexp"
	"sync"

	"github.com/replicase/pgcapture/pkg/cursor"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/pgcapture"
	"github.com/replicase/pgcapture/pkg/source"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// Gateway chang从pulsar获取，dumper从agent获取
type Gateway struct {
	pb.UnimplementedDBLogGatewayServer
	SourceResolver SourceResolver //下载change
	DumpInfoPuller DumpInfoPuller //下载元信息的请求和获取
}

func (s *Gateway) Serve(ctx context.Context, ln net.Listener, opts ...grpc.ServerOption) error {
	server := grpc.NewServer(opts...)
	pb.RegisterDBLogGatewayServer(server, s)

	if ch := ctx.Done(); ch != nil {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func(ctx context.Context) {
			<-ctx.Done()
			server.GracefulStop()
		}(ctx)
	}

	return server.Serve(ln)
}

// Capture 下游通过server来流式消费change
func (s *Gateway) Capture(server pb.DBLogGateway_CaptureServer) error {
	request, err := server.Recv()
	if err != nil {
		return err
	}

	// 获取Init请求
	init := request.GetInit()
	if init == nil {
		return ErrCaptureInitMessageRequired
	}

	// 正则过滤器
	filter, err := tableRegexFromInit(init)
	if err != nil {
		return err
	}

	// pulsar
	src, err := s.SourceResolver.Source(server.Context(), init.Uri)
	if err != nil {
		return err
	}
	// agent
	dumper, err := s.SourceResolver.Dumper(server.Context(), init.Uri)
	if err != nil {
		return err
	}
	defer dumper.Stop()

	return s.capture(init, filter, server, src, dumper)
}

// 接收应用消费信息
func (s *Gateway) acknowledge(server pb.DBLogGateway_CaptureServer, src source.RequeueSource, dumps *dumpMap) chan error {
	done := make(chan error)
	go func() {
		for {
			request, err := server.Recv()
			if err != nil {
				done <- err
				close(done)
				return
			}
			// 获取ack请求
			if ack := request.GetAck(); ack != nil {
				if ack.Checkpoint.Lsn != 0 {
					if ack.RequeueReason != "" {
						src.Requeue(cursor.Checkpoint{
							// 消费事件失败，重新入队，重新消费
							LSN:  ack.Checkpoint.Lsn,
							Seq:  ack.Checkpoint.Seq,
							Data: ack.Checkpoint.Data,
						}, ack.RequeueReason)
					} else {
						// 表示成功消费事件
						src.Commit(cursor.Checkpoint{
							LSN:  ack.Checkpoint.Lsn,
							Seq:  ack.Checkpoint.Seq,
							Data: ack.Checkpoint.Data,
						})
					}
				} else {
					// ack.Checkpoint.Lsn == 0 for dumps
					// ack.Checkpoint.Seq is dump id
					// len(ack.Checkpoint.Data) == 1 for last record of dump
					if len(ack.Checkpoint.Data) == 1 {
						// Lsn == 0 && len(ack.Checkpoint.Data) == 1 => 表示消费到最后的change
						dumps.ack(ack.Checkpoint.Seq, ack.RequeueReason)
					} else if ack.RequeueReason != "" {
						// Lsn == 0 && ack>RequeueReason != "" =>
						dumps.ack(ack.Checkpoint.Seq, ack.RequeueReason)
					}
				}
			}
		}
	}()
	return done
}

// DBLogGateway_CaptureServer接收两种请求：1.init请求 2.ack请求 发送：changes回复
func (s *Gateway) capture(init *pb.CaptureInit, filter *regexp.Regexp, server pb.DBLogGateway_CaptureServer, src source.RequeueSource, dumper SourceDumper) error {
	var addr string
	if p, ok := peer.FromContext(server.Context()); ok {
		addr = p.Addr.String()
	}
	logger := logrus.WithFields(logrus.Fields{"URI": init.Uri, "From": "Gateway", "Peer": addr})

	changes, err := src.Capture(cursor.Checkpoint{})
	if err != nil {
		return err
	}

	once := sync.Once{}
	cleanUp := func() {
		logger.Infof("stoping pulsar source")
		go func() {
			for range changes {
				// this loop should do nothing and only exit when the input channel is closed
			}
		}()
		src.Stop()
		logger.Infof("pulsar source stopped")
	}
	go func() {
		<-server.Context().Done()
		once.Do(cleanUp)
	}()
	defer func() {
		once.Do(cleanUp)
	}()
	logger.Infof("start capturing")

	ongoingDumps := &dumpMap{m: make(map[uint32]DumpInfo, 2)}
	done := s.acknowledge(server, src, ongoingDumps)
	dumps := s.DumpInfoPuller.Pull(server.Context(), init.Uri)

	lsn := uint64(0)

	// 异步获取changes,dumps_info
	for {
		select {
		case <-server.Context().Done():
			return server.Context().Err()
		case msg, more := <-changes:
			if !more {
				return nil
			}
			// 发送changes到consumer
			if change := msg.Message.GetChange(); change != nil && (filter == nil || filter.MatchString(change.Table)) {
				if err := server.Send(&pb.CaptureMessage{Checkpoint: &pb.Checkpoint{
					Lsn:  msg.Checkpoint.LSN,
					Seq:  msg.Checkpoint.Seq,
					Data: msg.Checkpoint.Data,
				}, Change: change}); err != nil {
					return err
				}
			} else {
				src.Commit(cursor.Checkpoint{
					LSN:  msg.Checkpoint.LSN,
					Seq:  msg.Checkpoint.Seq,
					Data: msg.Checkpoint.Data,
				})
			}
			lsn = msg.Checkpoint.LSN
		case info, more := <-dumps:
			if !more {
				return nil
			}
			var changes []*pb.Change
			if filter == nil || filter.MatchString(info.Resp.Table) {
				changes, err = dumper.LoadDump(lsn, info.Resp)
				if err != nil {
					logger.WithFields(logrus.Fields{"Dump": info.Resp.String()}).Errorf("dump error %v", err)
					if !errors.Is(err, ErrMissingTable) {
						info.Ack(err.Error())
						continue
					}
				}
			}
			if len(changes) == 0 {
				info.Ack("")
				continue
			}

			//发送dumps的records到consumer，通过dumpID标识下载快照信息的是属于哪个请求的
			var isLast []byte
			dumpID := ongoingDumps.store(info)
			for i, change := range changes {
				//标识是否是最后的change
				if i+1 == len(changes) {
					isLast = []byte{1}
				}
				if err = server.Send(&pb.CaptureMessage{Checkpoint: &pb.Checkpoint{
					Lsn:  0,
					Seq:  dumpID,
					Data: isLast,
				}, Change: change}); err != nil {
					logger.WithFields(logrus.Fields{"Dump": info.Resp.String(), "Len": len(changes), "Idx": i}).Errorf("partial dump error: %v", err)
					info.Ack(err.Error())
					return err
				}
			}
		case err := <-done:
			return err
		}
	}
}

// 返回符合正则表达式的regexp
func tableRegexFromInit(init *pb.CaptureInit) (*regexp.Regexp, error) {
	if init.Parameters == nil || init.Parameters.Fields == nil {
		return nil, nil
	}
	if regex, ok := init.Parameters.Fields[pgcapture.TableRegexOption]; ok {
		return regexp.Compile(regex.GetStringValue())
	}
	return nil, nil
}

var (
	ErrCaptureInitMessageRequired = errors.New("the first request should be a CaptureInit message")
)

type dumpMap struct {
	mu sync.Mutex
	m  map[uint32]DumpInfo
}

func (m *dumpMap) store(info DumpInfo) uint32 {
	id := dumpID(info.Resp)
	m.mu.Lock()
	m.m[id] = info
	m.mu.Unlock()
	return id
}

func (m *dumpMap) ack(id uint32, reason string) {
	m.mu.Lock()
	if info, ok := m.m[id]; ok {
		info.Ack(reason)
		delete(m.m, id)
	}
	m.mu.Unlock()
}

// hash code
func dumpID(info *pb.DumpInfoResponse) uint32 {
	sum := crc32.NewIEEE()
	sum.Write([]byte(info.Schema))
	sum.Write([]byte(info.Table))
	binary.Write(sum, binary.BigEndian, info.PageBegin)
	binary.Write(sum, binary.BigEndian, info.PageEnd)
	return sum.Sum32()
}
