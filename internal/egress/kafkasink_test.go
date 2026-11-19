package egress

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kfake"
)

func TestKafkaSinkProduces(t *testing.T) {
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.DefaultNumPartitions(2))
	defer cluster.Close()
	broker := cluster.ListenAddrs()[0]
	if err := cluster.CreateTopic("dest", 2, nil); err != nil {
		t.Fatal(err)
	}

	r := Route{Name: "to-kafka"}
	r.Sink = SinkConfig{Type: "kafka", Brokers: []string{broker}, Topic: "dest"}
	sink, err := NewSink(r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sink.Close() }()
	batch := []Delivery{
		{Key: []byte("k1"), Value: []byte("v1"), Offset: 0, Partition: 0, Headers: map[string]string{"origin": "test"}},
		{Key: []byte("k2"), Value: []byte("v2"), Offset: 1, Partition: 0},
	}
	if err := sink.Write(context.Background(), "to-kafka", batch); err != nil {
		t.Fatal(err)
	}

	// Read everything back from the fake broker.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics("dest"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seen := map[string]string{}
	for len(seen) < 2 {
		for _, rec := range cl.PollRecords(ctx, 10).Records() {
			seen[string(rec.Key)] = string(rec.Value)
			if len(rec.Headers) > 0 {
				if rec.Headers[0].Key != "origin" {
					t.Fatalf("header %q", rec.Headers[0].Key)
				}
			}
		}
	}
	if seen["k1"] != "v1" || seen["k2"] != "v2" {
		t.Fatalf("records wrong: %v", seen)
	}
}

func TestKafkaSinkRequiresBrokersAndTopic(t *testing.T) {
	r := Route{Name: "x"}
	r.Sink = SinkConfig{Type: "kafka"}
	if _, err := NewSink(r); err == nil {
		t.Fatal("kafka sink without brokers must fail validation/construction")
	}
}

