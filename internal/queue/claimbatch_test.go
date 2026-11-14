package queue

// Acceptance tests for plan 006 slice 6 (batch claim): a pull that claims N
// deliveries must persist the attempt records for ALL of them in one
// fsync-equivalent — one AppendBatch call, zero per-record Appends — and a
// failed batch must leave nothing claimed, pending, or counted.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/log"
	"github.com/rahulkj/firn/internal/meta"
)

// countingAttemptLog wraps an attempt log and counts the calls that hit it.
// failBatch makes AppendBatch return an error (simulated fsync failure).
type countingAttemptLog struct {
	inner     attemptLog
	appends   int
	batches   int
	failBatch error
}

func (c *countingAttemptLog) Append(key, value []byte) int64 {
	c.appends++
	return c.inner.Append(key, value)
}
func (c *countingAttemptLog) AppendBatch(recs []log.BatchRecord) ([]int64, error) {
	c.batches++
	if c.failBatch != nil {
		return nil, c.failBatch
	}
	if f, ok := c.inner.(*log.FileLog); ok {
		return f.AppendBatch(recs)
	}
	return c.inner.AppendBatch(recs)
}
func (c *countingAttemptLog) Fetch(offset int64, max int) ([]log.Record, error) {
	return c.inner.Fetch(offset, max)
}
func (c *countingAttemptLog) HighWatermark() int64 { return c.inner.HighWatermark() }

// TestPullClaimsAttemptsInOneBatch: claiming N deliveries routes exactly one
// AppendBatch call and zero per-record Appends through the attempts log.
func TestPullClaimsAttemptsInOneBatch(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()
	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	store, err := NewAttemptStore("", nil)
	if err != nil {
		t.Fatalf("attempts store: %v", err)
	}
	counting := &countingAttemptLog{inner: store.inner}
	store.inner = counting // swap for the counting wrapper; keep the initialized cache
	m.attempts = store

	for i := 0; i < 15; i++ {
		if _, _, err := m.broker.Produce(ctx, "jobs", nil, []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	deliveries, err := m.Pull(ctx, "jobs", 10, 30*time.Second)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(deliveries) != 10 {
		t.Fatalf("pulled %d, want 10", len(deliveries))
	}
	if counting.batches != 1 {
		t.Fatalf("attempts log saw %d batches, want exactly 1", counting.batches)
	}
	if counting.appends != 0 {
		t.Fatalf("attempts log saw %d per-record Appends, want 0 (batch claim)", counting.appends)
	}
	// Attempt counts must still be right: 10 claims, attempts == 1 each.
	for _, d := range deliveries {
		if d.Attempts != 1 {
			t.Fatalf("delivery %s attempts = %d, want 1", d.DeliveryID, d.Attempts)
		}
	}
}

// TestPullAttemptsBatchFailureLeavesNothingPending: a failed attempts write
// must fail the pull with nothing delivered, pending, or counted.
func TestPullAttemptsBatchFailureLeavesNothingPending(t *testing.T) {
	m, reg := newTestQueue(t, "")
	ctx := context.Background()
	if err := reg.CreateWithMode("jobs", 1, meta.ModeQueue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	store, err := NewAttemptStore("", nil)
	if err != nil {
		t.Fatalf("attempts store: %v", err)
	}
	counting := &countingAttemptLog{inner: store.inner, failBatch: fmt.Errorf("injected attempts sync failure")}
	store.inner = counting // swap for the counting wrapper; keep the initialized cache
	m.attempts = store

	for i := 0; i < 5; i++ {
		if _, _, err := m.broker.Produce(ctx, "jobs", nil, []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	if d, err := m.Pull(ctx, "jobs", 5, 30*time.Second); err == nil {
		t.Fatalf("pull must fail when the attempts batch fails, got %d deliveries", len(d))
	}
	if got := m.attempts.Attempts("jobs", 0, 0); got != 0 {
		t.Fatalf("attempts[0] = %d after failed pull, want 0", got)
	}
	if got := m.attempts.Attempts("jobs", 0, 4); got != 0 {
		t.Fatalf("attempts[4] = %d after failed pull, want 0", got)
	}
	q := m.queues["jobs"]
	if len(q.pending) != 0 {
		t.Fatalf("pending holds %d deliveries after failed pull, want 0", len(q.pending))
	}
}
