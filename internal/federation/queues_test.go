package federation_test

// Active-active queue federation (ADR-010, slice 5). Both clusters converge
// on jobs records re-origined, both deliver to their own consumers, attempts
// mirror both ways with re-keyed source offsets, and a starved cluster
// diverges to the DLQ instead of losing messages. Competing consumers on
// both sides: every message consumed exactly once per cluster or DLQ'd.

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/queue"
)

func dlqKeys(t *testing.T, b *broker.Broker, queue string) []string {
	t.Helper()
	hw, err := b.PartitionHighWatermark(context.Background(), queue+".dlq", 0)
	if err != nil {
		return nil // topic not created yet
	}
	recs, err := b.Fetch(context.Background(), queue+".dlq", 0, 0, int(hw))
	if err != nil {
		t.Fatalf("dlq fetch %s: %v", queue, err)
	}
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, string(r.Key))
	}
	sort.Strings(out)
	return out
}

func TestActiveActiveQueueDeliveryAndDLQConverge(t *testing.T) {
	a, b := mustQueueClusters(t, "jobs", 1)

	ctx := context.Background()
	const n = 4
	for i := 0; i < n; i++ {
		if _, _, err := a.broker.Produce(ctx, "jobs", []byte(fmt.Sprintf("job-%d", i)), []byte(fmt.Sprintf("work-%d", i))); err != nil {
			t.Fatalf("produce on A: %v", err)
		}
	}

	// Wait for the jobs records to reach B (re-origined).
	waitFor(t, 30*time.Second, func() bool {
		hw, err := b.broker.PartitionHighWatermark(ctx, "jobs", 0)
		return err == nil && hw == int64(n)
	})

	aQM, err := queue.NewManager(a.reg, a.broker, a.dir)
	if err != nil {
		t.Fatalf("queue manager A: %v", err)
	}
	defer func() { _ = aQM.Close() }()
	bQM, err := queue.NewManager(b.reg, b.broker, b.dir)
	if err != nil {
		t.Fatalf("queue manager B: %v", err)
	}
	defer func() { _ = bQM.Close() }()

	// Competing consumers on both sides; 1ms visibility so leases lapse
	// instantly and attempts escalate. No acks anywhere — at-least-once per
	// cluster; divergent clusters land the same jobs in their DLQs.
	for round := 0; round < 8; round++ {
		if _, err := aQM.Pull(ctx, "jobs", n, time.Millisecond); err != nil {
			t.Fatalf("pull A: %v", err)
		}
		if _, err := bQM.Pull(ctx, "jobs", n, time.Millisecond); err != nil {
			t.Fatalf("pull B: %v", err)
		}
	}

	// Converge: both clusters' DLQs must contain the same four jobs.
	waitFor(t, 30*time.Second, func() bool {
		ka := dlqKeys(t, a.broker, "jobs")
		kb := dlqKeys(t, b.broker, "jobs")
		return len(ka) == n && len(kb) == n
	})
	ka := dlqKeys(t, a.broker, "jobs")
	kb := dlqKeys(t, b.broker, "jobs")
	if len(ka) != n || len(kb) != n {
		t.Fatalf("DLQ contents diverge: A=%v B=%v", ka, kb)
	}
	for i := range ka {
		if ka[i] != kb[i] {
			t.Fatalf("DLQ key mismatch at %d: A=%v B=%v", i, ka, kb)
		}
	}
}

func TestActiveActiveQueueAttemptsMirrorRoundTrip(t *testing.T) {
	a, b := mustQueueClusters(t, "jobs", 1)

	ctx := context.Background()
	const n = 2
	for i := 0; i < n; i++ {
		if _, _, err := a.broker.Produce(ctx, "jobs", []byte(fmt.Sprintf("job-%d", i)), []byte(fmt.Sprintf("work-%d", i))); err != nil {
			t.Fatalf("produce on A: %v", err)
		}
	}
	waitFor(t, 30*time.Second, func() bool {
		hw, err := b.broker.PartitionHighWatermark(ctx, "jobs", 0)
		return err == nil && hw == int64(n)
	})

	aQM, err := queue.NewManager(a.reg, a.broker, a.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = aQM.Close() }()
	bQM, err := queue.NewManager(b.reg, b.broker, b.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bQM.Close() }()

	// One delivery on B: B claims the jobs; the attempts must flow back to A
	// and translate to A's local offsets (re-origin mapping).
	deliveries, err := bQM.Pull(ctx, "jobs", n, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != n {
		t.Fatalf("B delivered %d, want %d", len(deliveries), n)
	}
	// Attempt counts converge within lag: A's attempts for each A-offset
	// must reach 1 once the mirror flows back.
	waitFor(t, 30*time.Second, func() bool { return attemptsReached(aQM, "jobs", n, 1) })
	if !attemptsReached(aQM, "jobs", n, 1) {
		t.Fatalf("attempts did not mirror back to A within lag")
	}
}

// attemptsReached reports whether A's folded attempts for every offset
// of the queue have reached min — i.e. B's deliveries mirrored back and
// were re-keyed into A's local offset space.
func attemptsReached(qm *queue.Manager, queue string, n int, min uint32) bool {
	for off := int64(0); off < int64(n); off++ {
		if qm.Attempts(queue, 0, off) < min {
			return false
		}
	}
	return true
}
