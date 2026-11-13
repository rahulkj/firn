package replication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/fence"
	"github.com/rahulkj/firn/internal/meta"
)

// TestInternalTopicSingleWriter is the Phase F completion gate: with the
// internal lease bound into every node's write fence, only the lease holder
// may append to __consumer_offsets. A concurrent non-holder write fails
// closed, appends nothing, and the replicated bytes converge on every node.
func TestInternalTopicSingleWriter(t *testing.T) {
	root := t.TempDir()
	nodes := newCluster(t, 3, root)
	reg := nodes[0].reg

	if err := reg.CreateWithMode("orders", 1, meta.ModeStream); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Bind each node's internal-topic fence to a quorum-backed advisor.
	for _, cn := range nodes {
		adv := NewAdvisor(cn.id, cn.q, 2*time.Second)
		cn.b.InternalGateBox().Set(adv.InternalGate())
	}

	// Wait for the internal lease on __consumer_offsets to be granted.
	holderIdx := -1
	waitFor(t, "internal lease granted", 5*time.Second, func() bool {
		for i, cn := range nodes {
			if l, ok := cn.q.Lease("__consumer_offsets", 0); ok && l.Holder != "" && l.Holder == cn.id {
				holderIdx = i
				return true
			}
		}
		return false
	})
	holder := nodes[holderIdx]
	nonHolder := nodes[(holderIdx+1)%len(nodes)]

	// Non-holder: the write must fail closed with a fence error and the
	// replica must stay empty.
	hwBefore := nonHolderHwm(t, nonHolder)
	if err := nonHolder.b.Groups().CommitOffset("gB", "orders", 0, 1, 1, false); err == nil {
		t.Fatal("non-holder commit succeeded")
	} else if !errors.Is(err, fence.ErrFenced) {
		t.Fatalf("non-holder commit error = %v, want fence.ErrFenced", err)
	}
	if hw := nonHolderHwm(t, nonHolder); hw != hwBefore {
		t.Fatalf("non-holder HWM moved %d -> %d on fenced write", hwBefore, hw)
	}

	// Holder: the commit succeeds.
	if err := holder.b.Groups().CommitOffset("gA", "orders", 0, 1, 1, false); err != nil {
		t.Fatalf("holder commit: %v", err)
	}

	// Wait for the block commit to be visible on every node.
	waitFor(t, "__consumer_offsets replicated to all nodes", 10*time.Second, func() bool {
		for _, cn := range nodes {
			fl, ok := cn.b.InternalLog("__consumer_offsets")
			if !ok || fl.HighWatermark() != 1 {
				return false
			}
		}
		return true
	})

	// Every replica must be byte-identical.
	want := readInternalLog(t, holder.dataDir)
	for _, cn := range nodes {
		got := readInternalLog(t, cn.dataDir)
		if len(got) != len(want) {
			t.Fatalf("node %s: internal log length = %d, want %d", cn.id, len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("node %s: internal log differs at byte %d", cn.id, i)
			}
		}
	}

	// Both committed reads (holder and replicated) agree.
	for _, cn := range nodes {
		v, found, err := cn.b.Groups().FetchOffset("gA", "orders", 0)
		if err != nil {
			t.Fatalf("node %s fetch offset: %v", cn.id, err)
		}
		if !found || v.Offset != 1 {
			t.Fatalf("node %s offset = %d/%v, want 1/true", cn.id, v.Offset, found)
		}
	}
}

func nonHolderHwm(t *testing.T, cn *clusterNode) int64 {
	t.Helper()
	fl, ok := cn.b.InternalLog("__consumer_offsets")
	if !ok {
		t.Fatal("internal log not opened")
	}
	return fl.HighWatermark()
}

func readInternalLog(t *testing.T, dir string) []byte {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "__consumer_offsets-0", "*.log"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("internal log segments not found under %s: %v", dir, err)
	}
	var out []byte
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			t.Fatalf("read segment %s: %v", m, err)
		}
		out = append(out, b...)
	}
	return out
}
