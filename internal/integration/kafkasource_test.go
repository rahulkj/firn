package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaPositionRoundTrip(t *testing.T) {
	for _, in := range []string{"", "0:0", "0:5,1:9,2:1", "10:12345"} {
		p, err := decodeKafkaPosition(in)
		if err != nil {
			t.Fatalf("decode %q: %v", in, err)
		}
		out := encodeKafkaPosition(p)
		p2, err := decodeKafkaPosition(out)
		if err != nil {
			t.Fatalf("re-decode %q: %v", out, err)
		}
		if len(p) != len(p2) {
			t.Fatalf("round trip changed size: %v -> %v", p, p2)
		}
		for k, v := range p {
			if p2[k] != v {
				t.Fatalf("round trip changed entry %d: %d -> %d", k, v, p2[k])
			}
		}
	}
	if _, err := decodeKafkaPosition("not-a-position"); err == nil {
		t.Fatal("bad token must be rejected")
	}
}

func TestKafkaSourceConnector(t *testing.T) {
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.DefaultNumPartitions(2))
	defer cluster.Close()
	broker := cluster.ListenAddrs()[0]
	if err := cluster.CreateTopic("inbox", 2, nil); err != nil {
		t.Fatal(err)
	}
	// Seed records into the fake cluster.
	prod, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		rec := &kgo.Record{Topic: "inbox", Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte(fmt.Sprintf("v%d", i))}
		prod.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce: %v", err)
			}
		})
	}
	if err := prod.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	prod.Close()

	conn, err := NewKafkaSourceConnector("test-kafka", []string{broker}, "inbox", "earliest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx := context.Background()
	seen := map[string]bool{}
	position := ""
	for len(seen) < 6 {
		recs, err := conn.FetchBatch(ctx, position, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) == 0 {
			t.Fatal("no records from kafka source yet (expected buffered seed data)")
		}
		for _, r := range recs {
			seen[string(r.Key)] = true
			if r.Position == "" {
				t.Fatal("record without position")
			}
			position = r.Position
		}
	}
	// Drained: next poll returns nothing.
	recs, err := conn.FetchBatch(ctx, position, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected drained source, got %d records", len(recs))
	}
	// Fresh connector from the advanced position sees nothing either.
	conn2, err := NewKafkaSourceConnector("test-kafka-2", []string{broker}, "inbox", "earliest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn2.Close() }()
	recs, err = conn2.FetchBatch(ctx, position, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("resumed connector saw %d records, want 0 (dupes would violate checkpoint contract)", len(recs))
	}
}

func TestKafkaSourceValidation(t *testing.T) {
	if _, err := NewKafkaSourceConnector("bad name!", []string{"b:9092"}, "t", "earliest"); err == nil {
		t.Fatal("bad name accepted")
	}
	if _, err := NewKafkaSourceConnector("ok", nil, "t", "earliest"); err == nil {
		t.Fatal("missing brokers accepted")
	}
	if _, err := NewKafkaSourceConnector("ok", []string{"b:9092"}, "t", "middle"); err == nil {
		t.Fatal("bad start accepted")
	}
}
