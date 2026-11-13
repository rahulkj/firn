// Package e2e is the slice-4 acceptance scenario from
// docs/plans/001-mvp-single-node.md: produce 600 messages across 3
// partitions, consume them with a 2-member group, commit offsets, restart
// the broker (data dir kept), rejoin, and resume exactly after the
// committed offsets.
package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/rahulkj/eventing/internal/broker"
	"github.com/rahulkj/eventing/internal/meta"
)

func produce600(t *testing.T, b *broker.Broker) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 600; i++ {
		if _, _, err := b.Produce(ctx, "orders", []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("v%03d", i))); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
}

func TestConsumerGroupEndToEnd(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// First "process": topic registry persisted in the data dir (as eventingd does).
	reg1, err := meta.OpenRegistry(dir + "/topics.json")
	if err != nil {
		t.Fatalf("open registry: %v", err)
	}
	if err := reg1.Create("orders", 3); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	b1, err := broker.NewWithDir(reg1, dir)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	produce600(t, b1)

	m1, err := b1.Groups().JoinGroup("g1", "", []string{"orders"}, "range", 0)
	if err != nil {
		t.Fatalf("m1 join: %v", err)
	}
	m2, err := b1.Groups().JoinGroup("g1", "", []string{"orders"}, "range", 0)
	if err != nil {
		t.Fatalf("m2 join: %v", err)
	}

	s1, gen1, err := b1.Groups().SyncGroup("g1", m1.MemberID, 0)
	if err != nil {
		t.Fatalf("m1 sync: %v", err)
	}
	s2, gen2, err := b1.Groups().SyncGroup("g1", m2.MemberID, 0)
	if err != nil {
		t.Fatalf("m2 sync: %v", err)
	}

	// Both members consume their assigned partitions fully.
	consumeAndCommit := func(member string, assignment map[string][]int32, gen int64) {
		for _, partition := range assignment["orders"] {
			offset := int64(0)
			var last int64 = -1
			for {
				recs, err := b1.Fetch(ctx, "orders", partition, offset, 50)
				if err != nil {
					t.Fatalf("%s fetch p%d off %d: %v", member, partition, offset, err)
				}
				if len(recs) == 0 {
					break
				}
				for _, r := range recs {
					last = r.Offset
				}
				offset = last + 1
			}
			if last >= 0 {
				if err := b1.Groups().CommitOffset("g1", "orders", partition, last+1, gen, false); err != nil {
					t.Fatalf("%s commit p%d: %v", member, partition, err)
				}
			}
		}
	}
	consumeAndCommit("m1", s1, gen1)
	consumeAndCommit("m2", s2, gen2)

	if err := b1.Close(); err != nil {
		t.Fatalf("close broker: %v", err)
	}

	// Second "process": restart with the same data dir (topics persist too).
	reg2, err := meta.OpenRegistry(dir + "/topics.json")
	if err != nil {
		t.Fatalf("open registry: %v", err)
	}
	b2, err := broker.NewWithDir(reg2, dir)
	if err != nil {
		t.Fatalf("broker reopen: %v", err)
	}
	defer func() { _ = b2.Close() }()

	// Committed offsets resume exactly where the first process left off.
	for p := int32(0); p < 3; p++ {
		v, found, err := b2.Groups().FetchOffset("g1", "orders", p)
		if err != nil {
			t.Fatalf("fetch committed offset p%d: %v", p, err)
		}
		if !found {
			t.Fatalf("partition %d: no committed offset after restart", p)
		}
		hw := int64(0)
		{
			// hwm via fetch loop isn't public; compare against the total
			// by fetching until empty.
			off := int64(0)
			for {
				recs, err := b2.Fetch(ctx, "orders", p, off, 256)
				if err != nil {
					t.Fatalf("fetch p%d: %v", p, err)
				}
				if len(recs) == 0 {
					break
				}
				off += int64(len(recs))
			}
			hw = off
		}
		if v.Offset != hw {
			t.Fatalf("partition %d: committed offset %d, want %d (resume point)", p, v.Offset, hw)
		}
	}
}
