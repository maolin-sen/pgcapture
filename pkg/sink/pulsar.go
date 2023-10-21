package sink

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rueian/pgcapture/pkg/cursor"
	"github.com/rueian/pgcapture/pkg/source"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type SetupTracker func(client pulsar.Client, topic string) (cursor.Tracker, error)

func setupDefaultTracker(client pulsar.Client, topic string) (cursor.Tracker, error) {
	return cursor.NewPulsarTracker(client, topic)
}

type PulsarSink struct {
	BaseSink

	PulsarOption pulsar.ClientOptions
	PulsarTopic  string
	// For overriding the cluster list to be replicated to
	ReplicatedClusters []string

	SetupTracker SetupTracker

	client     pulsar.Client   //pulsar client
	tracker    cursor.Tracker  //pulsar reader
	producer   pulsar.Producer //pulsar producer
	log        *logrus.Entry
	prev       cursor.Checkpoint
	consistent bool
}

// Setup 初始化
func (p *PulsarSink) Setup() (cp cursor.Checkpoint, err error) {
	p.client, err = pulsar.NewClient(p.PulsarOption)
	if err != nil {
		return cp, err
	}

	host, err := os.Hostname()
	if err != nil {
		return cp, err
	}

	if p.SetupTracker == nil {
		p.SetupTracker = setupDefaultTracker
	}

	p.tracker, err = p.SetupTracker(p.client, p.PulsarTopic)
	if err != nil {
		return cp, err
	}

	// Set up the producer first to avoid the existence of another producer when trying to read the latest message
	p.producer, err = p.client.CreateProducer(pulsar.ProducerOptions{
		Topic:               p.PulsarTopic,
		Name:                p.PulsarTopic + "-producer", // fixed for exclusive producer
		Properties:          map[string]string{"host": host},
		MaxPendingMessages:  2000,
		CompressionType:     pulsar.ZSTD,
		BatchingMaxMessages: 1000,
		BatchingMaxSize:     1024 * 1024,
	})
	if err != nil {
		return cp, err
	}

	cp, err = p.tracker.Last()
	if err != nil {
		return cp, err
	}
	p.prev = cp

	p.log = logrus.WithFields(logrus.Fields{
		"From":  "PulsarSink",
		"Topic": p.PulsarTopic,
	})
	p.log.WithFields(logrus.Fields{
		"SinkLastLSN": p.prev.LSN,
		"SinkLastSeq": p.prev.Seq,
	}).Info("start sending changes to pulsar")

	p.BaseSink.CleanFn = func() {
		p.producer.Flush()
		p.producer.Close()
		p.client.Close()
		p.tracker.Close()
	}

	return cp, nil
}

// Apply pulsar接收器将接收到的数据发送到队列
func (p *PulsarSink) Apply(changes chan source.Change) chan cursor.Checkpoint {
	p.tracker.Start()

	var first bool
	return p.BaseSink.apply(changes, func(change source.Change, committed chan cursor.Checkpoint) error {
		if !first {
			p.log.WithFields(logrus.Fields{
				"MessageLSN":         change.Checkpoint.LSN,
				"MessageSeq":         change.Checkpoint.Seq,
				"SinkLastLSN":        p.prev.LSN,
				"SinkLastSeq":        p.prev.Seq,
				"Message":            change.Message.String(),
				"ReplicatedClusters": p.ReplicatedClusters,
			}).Info("applying the first message from source")
			first = true
		}

		p.consistent = p.consistent || change.Checkpoint.After(p.prev)
		if !p.consistent {
			p.log.WithFields(logrus.Fields{
				"MessageLSN":         change.Checkpoint.LSN,
				"MessageSeq":         change.Checkpoint.Seq,
				"SinkLastLSN":        p.prev.LSN,
				"SinkLastSeq":        p.prev.Seq,
				"Message":            change.Message.String(),
				"ReplicatedClusters": p.ReplicatedClusters,
			}).Warn("message dropped due to its lsn smaller than the last lsn of sink")
			return nil
		}

		bs, err := proto.Marshal(change.Message)
		if err != nil {
			return err
		}

		//读取changes信息，然后发送到pulsar
		p.producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Key:                 change.Checkpoint.ToKey(), // for topic compaction, not routing policy
			Payload:             bs,
			ReplicationClusters: p.ReplicatedClusters,
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			var idHex string
			if id != nil {
				idHex = hex.EncodeToString(id.Serialize())
			}

			if err != nil {
				p.log.WithFields(logrus.Fields{
					"MessageLSN":         change.Checkpoint.LSN,
					"MessageIDHex":       idHex,
					"ReplicatedClusters": p.ReplicatedClusters,
				}).Errorf("fail to send message to pulsar: %v", err)
				p.BaseSink.err.Store(fmt.Errorf("%w", err))
				p.BaseSink.Stop()
				return
			}

			cp := change.Checkpoint
			if err := p.tracker.Commit(cp, id); err != nil {
				p.log.WithFields(logrus.Fields{
					"MessageLSN":   change.Checkpoint.LSN,
					"MessageSeq":   change.Checkpoint.Seq,
					"MessageIDHex": idHex,
				}).Errorf("fail to commit message to tracker: %v", err)
			}
			committed <- cp
		})
		return nil
	})
}
