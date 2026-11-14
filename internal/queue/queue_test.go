package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/meta"
)

func newTestQueue(t *testing.T, dir string) (*Manager, *meta.Registry) {
	t.Helper()
	var reg *meta.Registry
	var err error
	if dir == "" {
		reg = meta.NewRegistry()
	} else {
		reg, err = meta.OpenRegistry(dir + "/topics.json")
		if err != nil {
			t.Fatalf("open registry: %v", err)
		}
	}
	var b *broker.Broker
	if dir == "" {
		b = broker.New(reg)
	} else {
		var err error
		b, err = broker.NewWithDir(reg, dir)
		if err != nil {
			t.Fatalf("broker: %v", err)
		}
	}
	m, err := NewManager(reg, b, dir)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close()
		_ = b.Close()
	})
	return m, reg
}

func TestPullSplitsMessagesAcrossCompetingConsumers(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()

	if err := reg.CreateWithMode("jobs", 2, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	b := m.broker
	for i := 0; i < 10; i++ {
		if _, _, err := b.Produce(ctx, "jobs", []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	d1, err := m.Pull(ctx, "jobs", 5, 30*time.Second)
	if err != nil {
		t.Fatalf("pull 1: %v", err)
	}
	d2, err := m.Pull(ctx, "jobs", 5, 30*time.Second)
	if err != nil {
		t.Fatalf("pull 2: %v", err)
	}
	if len(d1)+len(d2) != 10 {
		t.Fatalf("pulled %d + %d, want 10 total", len(d1), len(d2))
	}
	seen := map[string]bool{}
	for _, d := range append(append([]Delivery{}, d1...), d2...) {
		if seen[string(d.Value)] {
			t.Fatalf("message %q delivered twice to competing consumers", d.Value)
		}
		seen[string(d.Value)] = true
	}
}

func TestUnackedMessageRedeliversAfterVisibilityTimeout(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()

	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, _, err := m.broker.Produce(ctx, "jobs", []byte("k"), []byte("unacked")); err != nil {
		t.Fatalf("produce: %v", err)
	}

	d1, err := m.Pull(ctx, "jobs", 1, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("pull 1: %v", err)
	}
	if len(d1) != 1 || string(d1[0].Value) != "unacked" {
		t.Fatalf("pull 1 = %+v", d1)
	}
	// Do not ack; let the lease expire.
	time.Sleep(150 * time.Millisecond)
	m.Expire()

	d2, err := m.Pull(ctx, "jobs", 1, 30*time.Second)
	if err != nil {
		t.Fatalf("pull 2: %v", err)
	}
	if len(d2) != 1 || string(d2[0].Value) != "unacked" {
		t.Fatalf("redelivery = %+v, want the same message", d2)
	}
	if d2[0].Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", d2[0].Attempts)
	}
	if d2[0].DeliveryID == d1[0].DeliveryID {
		t.Fatal("redelivery reused the old delivery id")
	}
}

func TestPoisonMessageLandsInDLQAfterMaxAttempts(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()

	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, _, err := m.broker.Produce(ctx, "jobs", []byte("poison"), []byte("bad")); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// Attempt 1-3: pull and drop (no ack), waiting for the lease to expire.
	for attempt := 1; attempt <= 3; attempt++ {
		d, err := m.Pull(ctx, "jobs", 1, 50*time.Millisecond)
		if err != nil {
			t.Fatalf("pull attempt %d: %v", attempt, err)
		}
		if len(d) != 1 {
			t.Fatalf("attempt %d: got %d deliveries, want 1", attempt, len(d))
		}
		if d[0].Attempts != int32(attempt) {
			t.Fatalf("attempt %d: attempts field = %d", attempt, d[0].Attempts)
		}
		time.Sleep(80 * time.Millisecond)
		m.Expire()
	}
	// Attempt 4 would exceed maxDeliveries: the message must go to the DLQ.
	d, err := m.Pull(ctx, "jobs", 1, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("pull attempt 4: %v", err)
	}
	if len(d) != 0 {
		t.Fatalf("attempt 4 delivered %+v, want the message diverted to DLQ", d)
	}
	// DLQ topic was auto-created and holds the poisoned message.
	dlq, err := reg.Get("jobs.dlq")
	if err != nil {
		t.Fatalf("dlq topic: %v", err)
	}
	_ = dlq
	recs, err := m.broker.Fetch(ctx, "jobs.dlq", 0, 0, 10)
	if err != nil {
		t.Fatalf("fetch dlq: %v", err)
	}
	if len(recs) != 1 || string(recs[0].Value) != "bad" || string(recs[0].Key) != "poison" {
		t.Fatalf("dlq contents = %+v", recs)
	}
	// The queue must be empty afterwards.
	d, err = m.Pull(ctx, "jobs", 10, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("final pull: %v", err)
	}
	if len(d) != 0 {
		t.Fatalf("queue still has deliveries: %+v", d)
	}
}

func TestAckUnknownDeliveryID(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()
	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if err := m.Ack(ctx, "jobs", "delivery-does-not-exist"); err == nil {
		t.Fatal("ack unknown delivery id must error")
	}
}

func TestAckOutOfOrderCommitsAdvanceOnlyContiguously(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()

	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, _, err := m.broker.Produce(ctx, "jobs", nil, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	d, err := m.Pull(ctx, "jobs", 3, 30*time.Second)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(d) != 3 {
		t.Fatalf("pulled %d, want 3", len(d))
	}
	// Ack the LAST message first: committed offset must NOT advance past
	// the first unacked message (v0).
	if err := m.Ack(ctx, "jobs", d[2].DeliveryID); err != nil {
		t.Fatalf("ack v2: %v", err)
	}
	off1, found, err := m.broker.Groups().FetchOffset("queue-jobs", "jobs", 0)
	if err != nil {
		t.Fatalf("fetch offset: %v", err)
	}
	if found && off1.Offset > 0 {
		t.Fatalf("committed offset advanced to %d while v0 unacked (want 0)", off1.Offset)
	}
	// Ack v0 and v1: the commit must advance to 3.
	if err := m.Ack(ctx, "jobs", d[0].DeliveryID); err != nil {
		t.Fatalf("ack v0: %v", err)
	}
	if err := m.Ack(ctx, "jobs", d[1].DeliveryID); err != nil {
		t.Fatalf("ack v1: %v", err)
	}
	off2, found, err := m.broker.Groups().FetchOffset("queue-jobs", "jobs", 0)
	if err != nil {
		t.Fatalf("fetch offset: %v", err)
	}
	if !found || off2.Offset != 3 {
		t.Fatalf("committed offset = %d found=%v, want 3 true", off2.Offset, found)
	}
}

func TestPendingDeliveriesRedeliveredAfterRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// First "process": produce and pull without acking.
	m1, reg := newTestQueue(t, dir)
	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, _, err := m1.broker.Produce(ctx, "jobs", nil, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	d1, err := m1.Pull(ctx, "jobs", 3, 30*time.Second)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(d1) != 3 {
		t.Fatalf("pulled %d, want 3", len(d1))
	}
	if err := m1.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}

	// Second "process": pending deliveries are re-visible.
	m2, _ := newTestQueue(t, dir)
	d2, err := m2.Pull(ctx, "jobs", 10, 30*time.Second)
	if err != nil {
		t.Fatalf("pull after restart: %v", err)
	}
	if len(d2) != 5 {
		t.Fatalf("after restart got %d deliveries, want 5 (3 redelivered + 2 fresh)", len(d2))
	}
	seen := map[string]int{}
	for _, d := range d2 {
		seen[string(d.Value)]++
	}
	for i := 0; i < 3; i++ {
		if seen[fmt.Sprintf("v%d", i)] != 1 {
			t.Fatalf("message v%d delivered %d times, want 1 (redelivery of in-flight)", i, seen[fmt.Sprintf("v%d", i)])
		}
	}
	for i := 3; i < 5; i++ {
		if seen[fmt.Sprintf("v%d", i)] != 1 {
			t.Fatalf("message v%d delivered %d times, want 1", i, seen[fmt.Sprintf("v%d", i)])
		}
	}
	// The 3 redelivered messages must carry attempts = 2.
	for _, d := range d2 {
		v := string(d.Value)
		if v == "v0" || v == "v1" || v == "v2" {
			if d.Attempts != 2 {
				t.Fatalf("redelivered %s attempts = %d, want 2", v, d.Attempts)
			}
		}
	}
}

func TestPullOnStreamTopicFails(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()
	if err := reg.Create("plain", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := m.Pull(ctx, "plain", 1, time.Second); err == nil {
		t.Fatal("pull on a stream topic must fail")
	}
}

func TestAckAdvancesGroupOffsets(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()
	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, _, err := m.broker.Produce(ctx, "jobs", nil, []byte("x")); err != nil {
		t.Fatalf("produce: %v", err)
	}
	d, err := m.Pull(ctx, "jobs", 1, 30*time.Second)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if err := m.Ack(ctx, "jobs", d[0].DeliveryID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	// The internal queue group offset must be committed at 1.
	off, found, err := m.broker.Groups().FetchOffset("queue-jobs", "jobs", 0)
	if err != nil || !found {
		t.Fatalf("fetch offset: found=%v err=%v", found, err)
	}
	if off.Offset != 1 {
		t.Fatalf("committed offset = %d, want 1", off.Offset)
	}
}

func TestAckBatchMixedKnownAndUnknown(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()
	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, _, err := m.broker.Produce(ctx, "jobs", nil, []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	deliveries, err := m.Pull(ctx, "jobs", 5, 30*time.Second)
	if err != nil || len(deliveries) != 5 {
		t.Fatalf("pull: n=%d err=%v", len(deliveries), err)
	}
	ids := []string{deliveries[0].DeliveryID, "bogus-id", deliveries[1].DeliveryID}
	res, err := m.AckBatch(ctx, "jobs", ids)
	if err != nil {
		t.Fatalf("ack batch: %v", err)
	}
	if res.Acked != 2 || res.Unknown != 1 {
		t.Fatalf("ack batch result = %+v, want {Acked:2 Unknown:1}", res)
	}
	if off, found, err := m.broker.Groups().FetchOffset("queue-jobs", "jobs", 0); err != nil || !found || off.Offset != 2 {
		t.Fatalf("committed offset = %+v found=%v, want 2", off, found)
	}
	// Remaining deliveries stay pending; settling them works the same way.
	res, err = m.AckBatch(ctx, "jobs", []string{deliveries[2].DeliveryID, deliveries[3].DeliveryID, deliveries[4].DeliveryID})
	if err != nil {
		t.Fatalf("ack batch 2: %v", err)
	}
	if res.Acked != 3 || res.Unknown != 0 {
		t.Fatalf("ack batch 2 result = %+v, want {Acked:3 Unknown:0}", res)
	}
	if off, found, err := m.broker.Groups().FetchOffset("queue-jobs", "jobs", 0); err != nil || !found || off.Offset != 5 {
		t.Fatalf("committed offset = %+v found=%v, want 5", off, found)
	}
}

func TestAckBatchEmptyAndOversize(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()
	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := m.AckBatch(ctx, "jobs", nil); err == nil {
		t.Fatal("empty batch must be rejected")
	}
	tooMany := make([]string, 501)
	if _, err := m.AckBatch(ctx, "jobs", tooMany); err == nil {
		t.Fatal(">500 ids must be rejected")
	}
}

func TestAckBatchOnStreamTopicRejected(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()
	if err := reg.Create("plain", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := m.AckBatch(ctx, "plain", []string{"x"}); err == nil {
		t.Fatal("AckBatch on a stream topic must be rejected")
	}
}
