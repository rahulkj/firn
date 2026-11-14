package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rahulkj/firn/internal/meta"
)

// TestBrokerWithDirSurvivesReopen is the slice-2 acceptance property for the
// broker layer: with a data dir configured, recreating the topic after a
// restart reopens existing partition logs — offsets are never reused
// (invariant 1) and no acknowledged record disappears.
func TestBrokerWithDirSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// First "process": create topic, produce.
	reg1 := meta.NewRegistry()
	if err := reg1.Create("events", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	b1, err := NewWithDir(reg1, dir)
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	for i := 0; i < 50; i++ {
		p, off, err := b1.Produce(ctx, "events", []byte(fmt.Sprintf("key-%d", i%7)), []byte(fmt.Sprintf("v%d", i)))
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		_ = p
		_ = off
	}
	if err := b1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Second "process": topics are in-memory until slice 3, so recreate the
	// topic; partition logs must reopen from disk with offsets preserved.
	reg2 := meta.NewRegistry()
	if err := reg2.Create("events", 2); err != nil {
		t.Fatalf("recreate topic: %v", err)
	}
	b2, err := NewWithDir(reg2, dir)
	if err != nil {
		t.Fatalf("broker reopen: %v", err)
	}
	defer func() { _ = b2.Close() }()

	var total int64
	counts := map[int32]int64{}
	for p := int32(0); p < 2; p++ {
		recs, err := b2.Fetch(ctx, "events", p, 0, 1000)
		if err != nil {
			t.Fatalf("fetch partition %d: %v", p, err)
		}
		for i, r := range recs {
			if r.Offset != int64(i) {
				t.Fatalf("partition %d record %d: offset = %d", p, i, r.Offset)
			}
		}
		counts[p] = int64(len(recs))
		total += int64(len(recs))
	}
	if total != 50 {
		t.Fatalf("fetched %d records after reopen, want 50", total)
	}

	// Produce after reopen must continue after the recovered offsets —
	// never reuse an offset.
	p0, off, err := b2.Produce(ctx, "events", []byte("key-0"), []byte("post-restart"))
	if err != nil {
		t.Fatalf("produce after reopen: %v", err)
	}
	if off != counts[p0] {
		t.Fatalf("post-restart offset = %d, want %d (recovered high watermark)", off, counts[p0])
	}
}

// ADR-014 slice 4: with -internal-flush-window, committed offsets are only
// durable after the window flushes — but a clean Close drains the window,
// so a clean restart sees every commit. (Crash-loss of up to the window is
// the documented, deliberate trade of the opt-in flag.)
func TestInternalFlushWindowCommitSurvivesCleanReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	reg1 := meta.NewRegistry()
	if err := reg1.Create("events", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	b1, err := NewWithDir(reg1, dir, WithInternalFlushWindow(20*time.Millisecond, 1<<20))
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	for i := 0; i < 17; i++ {
		if _, _, err := b1.Produce(ctx, "events", nil, []byte("v")); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	if err := b1.Groups().CommitOffset("g", "events", 0, 42, 0, false); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Commit happened long enough ago for its window to flush — but the
	// proof must not depend on timing: Close() drains the window.
	if err := b1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reg2 := meta.NewRegistry()
	if err := reg2.Create("events", 1); err != nil {
		t.Fatalf("recreate topic: %v", err)
	}
	b2, err := NewWithDir(reg2, dir, WithInternalFlushWindow(20*time.Millisecond, 1<<20))
	if err != nil {
		t.Fatalf("broker reopen: %v", err)
	}
	defer func() { _ = b2.Close() }()

	got, found, err := b2.Groups().FetchOffset("g", "events", 0)
	if err != nil {
		t.Fatalf("fetch offset: %v", err)
	}
	if !found || got.Offset != 42 {
		t.Fatalf("offset = %+v, found=%v; want 42", got, found)
	}
}
