package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rahulkj/slipway/internal/meta"
)

// TestInternalTopicReplicates ensures entries written to __consumer_offsets
// by one node are visible (replicated) on every node in the cluster.
//
// Scope/caveat: this exercises the single-writer case (one node owning a
// group commit). Multi-writer forks (two nodes committing their local
// copies for different groups concurrently) are a known edge deferred
// with phase F follow-ups.
func TestInternalTopicReplicates(t *testing.T) {
	root := t.TempDir()
	nodes := newCluster(t, 3, root)
	reg := nodes[0].reg

	if err := reg.CreateWithMode("orders", 1, meta.ModeStream); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	holderFor := func(topic string, partition int32) *clusterNode {
		for _, cn := range nodes {
			if NewAdvisor(cn.id, cn.q, time.Second).WriteAllowed(topic, partition) {
				return cn
			}
		}
		return nil
	}
	waitFor(t, "a writable node for orders/0", 5*time.Second, func() bool {
		return holderFor("orders", 0) != nil
	})
	holder := holderFor("orders", 0)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, _, err := holder.b.Produce(ctx, "orders", nil, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	if err := holder.b.Groups().CommitOffset("g1", "orders", 0, 3, 1, false); err != nil {
		t.Fatalf("commit offset: %v", err)
	}

	// Wait for the internal block commit to converge everywhere.
	waitFor(t, "__consumer_offsets committed on all nodes", 10*time.Second, func() bool {
		for _, cn := range nodes {
			if len(cn.q.Blocks("__consumer_offsets", 0)) == 0 {
				return false
			}
		}
		return true
	})

	// Verify the committed offset is visible on every node.
	for _, cn := range nodes {
		v, found, err := cn.b.Groups().FetchOffset("g1", "orders", 0)
		if err != nil {
			t.Fatalf("node %s fetch offset: %v", cn.id, err)
		}
		if !found || v.Offset != 3 {
			t.Fatalf("node %s committed offset = %d/%v, want 3/true", cn.id, v.Offset, found)
		}
	}
}
