package broker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rahulkj/firn/internal/meta"
)

func newTestBroker(t *testing.T, topics map[string]int) *Broker {
	t.Helper()
	reg := meta.NewRegistry()
	for name, partitions := range topics {
		if err := reg.Create(name, partitions); err != nil {
			t.Fatalf("create topic %q: %v", name, err)
		}
	}
	return New(reg)
}

func TestProduceAssignsPartitionByFNVKey(t *testing.T) {
	b := newTestBroker(t, map[string]int{"events": 4})
	ctx := context.Background()

	partitions := map[string]int32{}
	for _, key := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		p, _, err := b.Produce(ctx, "events", []byte(key), []byte("v"))
		if err != nil {
			t.Fatalf("produce: %v", err)
		}
		partitions[key] = p
	}
	// Same key must always land on the same partition.
	for key, want := range partitions {
		for i := 0; i < 8; i++ {
			got, _, err := b.Produce(ctx, "events", []byte(key), []byte("v"))
			if err != nil {
				t.Fatalf("produce: %v", err)
			}
			if got != want {
				t.Fatalf("key %q: partition moved %d -> %d", key, want, got)
			}
		}
	}
	// Offsets per partition must be monotonic from 0 (storage invariant 1).
	for p := int32(0); p < 4; p++ {
		recs, err := b.Fetch(ctx, "events", p, 0, 100)
		if err != nil {
			t.Fatalf("fetch partition %d: %v", p, err)
		}
		for i, r := range recs {
			if r.Offset != int64(i) {
				t.Fatalf("partition %d: record %d has offset %d", p, i, r.Offset)
			}
		}
	}
}

func TestProduceKeylessRoundRobins(t *testing.T) {
	b := newTestBroker(t, map[string]int{"events": 3})
	ctx := context.Background()

	seen := map[int32]bool{}
	for i := 0; i < 9; i++ {
		p, _, err := b.Produce(ctx, "events", nil, []byte(fmt.Sprintf("v%d", i)))
		if err != nil {
			t.Fatalf("produce: %v", err)
		}
		seen[p] = true
	}
	for p := int32(0); p < 3; p++ {
		if !seen[p] {
			t.Fatalf("partition %d never assigned for keyless produce", p)
		}
	}
}

func TestProduceUnknownTopic(t *testing.T) {
	b := newTestBroker(t, nil)
	_, _, err := b.Produce(context.Background(), "nope", []byte("k"), []byte("v"))
	if !errors.Is(err, meta.ErrTopicNotFound) {
		t.Fatalf("err = %v, want meta.ErrTopicNotFound", err)
	}
}

func TestFetchUnknownTopic(t *testing.T) {
	b := newTestBroker(t, nil)
	_, err := b.Fetch(context.Background(), "nope", 0, 0, 10)
	if !errors.Is(err, meta.ErrTopicNotFound) {
		t.Fatalf("err = %v, want meta.ErrTopicNotFound", err)
	}
}

func TestFetchPartitionOutOfRange(t *testing.T) {
	b := newTestBroker(t, map[string]int{"events": 2})
	_, err := b.Fetch(context.Background(), "events", 2, 0, 10)
	if !errors.Is(err, ErrPartitionOutOfRange) {
		t.Fatalf("err = %v, want ErrPartitionOutOfRange (partition == partitions)", err)
	}
	_, err = b.Fetch(context.Background(), "events", -1, 0, 10)
	if !errors.Is(err, ErrPartitionOutOfRange) {
		t.Fatalf("err = %v, want ErrPartitionOutOfRange", err)
	}
}

func TestFetchAtHighWatermarkReturnsEmpty(t *testing.T) {
	b := newTestBroker(t, map[string]int{"events": 1})
	ctx := context.Background()

	recs, err := b.Fetch(ctx, "events", 0, 0, 10)
	if err != nil {
		t.Fatalf("fetch empty partition: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("got %d records, want 0", len(recs))
	}

	if _, _, err := b.Produce(ctx, "events", []byte("k"), []byte("v")); err != nil {
		t.Fatalf("produce: %v", err)
	}
	recs, err = b.Fetch(ctx, "events", 0, 1, 10)
	if err != nil {
		t.Fatalf("fetch at high watermark: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("got %d records at high watermark, want 0", len(recs))
	}
}

func TestFetchBeyondHighWatermarkErrors(t *testing.T) {
	b := newTestBroker(t, map[string]int{"events": 1})
	_, err := b.Fetch(context.Background(), "events", 0, 5, 10)
	if err == nil {
		t.Fatal("expected error fetching beyond high watermark")
	}
}

func TestFetchNeverReturnsOffsetAtOrAboveHighWatermark(t *testing.T) {
	b := newTestBroker(t, map[string]int{"events": 1})
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if _, _, err := b.Produce(ctx, "events", []byte("k"), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	recs, err := b.Fetch(ctx, "events", 0, 0, 100)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	hw := int64(len(recs))
	for _, r := range recs {
		if r.Offset >= hw {
			t.Fatalf("record offset %d >= high watermark %d (invariant 2)", r.Offset, hw)
		}
	}
}

func TestProduceFetchRoundTripPreservesBytes(t *testing.T) {
	b := newTestBroker(t, map[string]int{"events": 1})
	ctx := context.Background()

	payloads := [][]byte{
		[]byte("plain"),
		{0x00, 0x01, 0xff, 0x00},
		[]byte(""),
	}
	for _, v := range payloads {
		if _, _, err := b.Produce(ctx, "events", []byte("k"), v); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	recs, err := b.Fetch(ctx, "events", 0, 0, 10)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(recs) != len(payloads) {
		t.Fatalf("got %d records, want %d", len(recs), len(payloads))
	}
	for i, r := range recs {
		if string(r.Value) != string(payloads[i]) {
			t.Fatalf("record %d value mismatch", i)
		}
	}
}

func TestFederationHWMDefaultZeroAndSettable(t *testing.T) {
	b := newTestBroker(t, map[string]int{"orders": 1})
	if got := b.FederationHWM("orders", 0); got != 0 {
		t.Fatalf("unhooked FederationHWM = %d, want 0", got)
	}
	b.SetFederationHWM(func(topic string, partition int32) int64 {
		if topic == "orders" && partition == 0 {
			return 41
		}
		return 0
	})
	if got := b.FederationHWM("orders", 0); got != 41 {
		t.Fatalf("hooked FederationHWM = %d, want 41", got)
	}
	if got := b.FederationHWM("orders", 1); got != 0 {
		t.Fatalf("uncovered partition FederationHWM = %d, want 0", got)
	}
}
