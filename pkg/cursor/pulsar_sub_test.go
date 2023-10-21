package cursor

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/replicase/pgcapture/internal/test"
)

func newPulsarSubscriptionTracker(topic string) (*PulsarSubscriptionTracker, func(), error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: test.GetPulsarURL(),
	})
	if err != nil {
		return nil, nil, err
	}

	tracker, err := NewPulsarSubscriptionTracker(client, topic, 100*time.Millisecond, false)
	if err != nil {
		client.Close()
		return nil, nil, err
	}

	closeFunc := func() {
		tracker.Close()
		client.Close()
	}
	return tracker, closeFunc, nil
}

func TestPulsarSubscriptionTracker_Commit(t *testing.T) {
	topic := time.Now().Format("20060102150405") + "-commit"

	tracker, cancel, err := newPulsarSubscriptionTracker(topic)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	// so it will start the commit loop
	tracker.Start()

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: test.GetPulsarURL(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
		Name:  topic + "-producer",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		defer producer.Close()
		_ = producer.Flush()
	}()

	var pos pulsar.MessageID

	for i := 0; i < 10; i++ {
		cp := Checkpoint{LSN: uint64(i + 100)}
		mid, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Key:     cp.ToKey(),
			Payload: []byte("test-" + strconv.Itoa(i)),
		})
		if err != nil {
			t.Fatal(err)
		}

		// set the position to the 5th message
		if i == 4 {
			pos = mid
		}
	}

	if err := tracker.Commit(Checkpoint{}, pos); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	admin, err := NewAdminClient()
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := CheckSubscriptionCursor(admin, topic, "pulsar-subscription-tracker-consumer")
	if err != nil {
		t.Fatal(err)
	}

	cp, err := GetCheckpointByMessageID(topic, cursor)
	if err != nil {
		t.Fatal(err)
	}

	// the next message should be the 6th message
	if cp.LSN != 105 {
		t.Fatalf("unexpected next position: %v", cp.LSN)
	}
}

func TestPulsarSubscriptionTracker_Last(t *testing.T) {
	topic := time.Now().Format("20060102150405")

	tracker, cancel, err := newPulsarSubscriptionTracker(topic)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: test.GetPulsarURL(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
		Name:  topic + "-producer",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		defer producer.Close()
		_ = producer.Flush()
	}()

	for i := 0; i < 10; i++ {
		cp := Checkpoint{LSN: uint64(i + 100)}
		_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Key:     cp.ToKey(),
			Payload: []byte("test-" + strconv.Itoa(i)),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	last, err := tracker.Last()
	if err != nil {
		t.Fatal(err)
	}
	if last.LSN != 109 {
		t.Fatalf("unexpected checkpoint.LSN: %v", last.LSN)
	}
}

func TestPulsarSubscriptionTracker_LastWithEmptyTopic(t *testing.T) {
	topic := time.Now().Format("20060102150405") + "-empty"

	tracker, cancel, err := newPulsarSubscriptionTracker(topic)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	last, err := tracker.Last()
	if err != nil {
		t.Fatal(err)
	}
	if last.LSN != 0 || last.Seq != 0 {
		t.Fatal("checkpoint of empty topic should be zero")
	}
}
