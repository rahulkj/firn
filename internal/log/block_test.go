package log

import (
	"fmt"
	"testing"
	"time"
)

func TestBlocksSealedOnSizeRollHaveCorrectBounds(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(256))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = l.Close() }()

	payload := []byte("0123456789abcdef")
	for i := 0; i < 60; i++ {
		l.Append([]byte("k"), payload)
	}
	blocks := l.Blocks()
	if len(blocks) == 0 {
		t.Fatal("want at least one sealed block")
	}
	var wantFirst int64
	for i, b := range blocks {
		if b.FirstOffset != wantFirst {
			t.Fatalf("block %d: FirstOffset = %d, want %d", i, b.FirstOffset, wantFirst)
		}
		if b.LastOffset < b.FirstOffset {
			t.Fatalf("block %d: inverted offsets %d-%d", i, b.FirstOffset, b.LastOffset)
		}
		if b.Size <= 0 {
			t.Fatalf("block %d: size = %d", i, b.Size)
		}
		if b.RecordCount <= 0 {
			t.Fatalf("block %d: no records", i)
		}
		wantFirst = b.LastOffset + 1
	}
	// The active (unsealed) tail must not appear.
	total := blocks[len(blocks)-1].LastOffset + 1 - blocks[0].FirstOffset
	if total != wantFirst {
		t.Fatalf("sealed blocks cover %d records, want %d", total, wantFirst)
	}
	if blocks[0].FirstOffset != 0 {
		t.Fatalf("first block starts at %d, want 0", blocks[0].FirstOffset)
	}
}

func TestBlockSealOnTimerRollsEarly(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithBlockSealing(60*time.Millisecond))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = l.Close() }()

	l.Append([]byte("k"), []byte("v1"))
	time.Sleep(80 * time.Millisecond)
	l.Append([]byte("k"), []byte("v2")) // triggers the time-based seal

	blocks := l.Blocks()
	if len(blocks) != 1 {
		t.Fatalf("blocks after timer seal = %d, want 1 (the first block)", len(blocks))
	}
	if got := blocks[0].LastOffset; got != 0 {
		t.Fatalf("timer-sealed block covers through offset %d, want 0", got)
	}
}

func TestBlockDescriptorDeterministicAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(256))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	payload := []byte("0123456789abcdef")
	for i := 0; i < 60; i++ {
		l.Append([]byte("k"), payload)
	}
	before := l.Blocks()
	if len(before) == 0 {
		t.Fatal("no blocks sealed")
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	l2, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(256))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()
	after := l2.Blocks()
	if len(after) != len(before) {
		t.Fatalf("blocks after reopen = %d, want %d", len(after), len(before))
	}
	for i := range before {
		if before[i].ID != after[i].ID {
			t.Fatalf("block %d ID changed across reopen", i)
		}
		if before[i].FirstOffset != after[i].FirstOffset || before[i].LastOffset != after[i].LastOffset {
			t.Fatalf("block %d offsets changed across reopen", i)
		}
	}
}

// Block IDs are content-addressed over the raw frame bytes (which include
// the record timestamp) — a freshly built block is identified by its actual
// on-disk content. Deterministic identity requires identical bytes, so the
// test pins timestamps with the internal appendWithTimestamp hook.
func TestBlockDescriptorIsContentAddressed(t *testing.T) {
	build := func(payload []byte) []BlockDescriptor {
		dir := t.TempDir()
		l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(256))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
		for i := 0; i < 60; i++ {
			l.appendWithTimestamp([]byte("k"), payload, base.Add(time.Duration(i)*time.Second).UnixMilli())
		}
		b := l.Blocks()
		_ = l.Close()
		return b
	}
	payloadA := []byte("0123456789abcdef")
	payloadB := []byte("fedcba9876543210")

	a1 := build(payloadA)
	a2 := build(payloadA)
	if len(a1) != len(a2) || len(a1) == 0 {
		t.Fatalf("descriptor sets differ in shape: %d vs %d", len(a1), len(a2))
	}
	for i := range a1 {
		if a1[i].ID != a2[i].ID {
			t.Fatalf("identical content produced different block IDs at %d", i)
		}
	}

	// Different payload -> different ID.
	c := build(payloadB)
	for i := range c {
		if i < len(a1) && a1[i].ID == c[i].ID {
			t.Fatalf("block %d ID collided across different content", i)
		}
	}
}

func TestBlocksExcludeActiveEmptySegment(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(256))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = l.Close() }()
	if got := l.Blocks(); len(got) != 0 {
		t.Fatalf("empty log reports %d blocks, want 0", len(got))
	}
}

func TestSealedBlocksNeverMutateOnFurtherAppends(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(256))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = l.Close() }()
	payload := []byte("0123456789abcdef")
	for i := 0; i < 60; i++ {
		l.Append([]byte("k"), payload)
	}
	before := l.Blocks()
	for i := 0; i < 20; i++ {
		l.Append([]byte("k"), []byte(fmt.Sprintf("tail-%d", i)))
	}
	after := l.Blocks()
	if len(after) < len(before) {
		t.Fatal("sealed block disappeared after appends")
	}
	for i := range before {
		if before[i].ID != after[i].ID {
			t.Fatalf("sealed block %d mutated after later appends", i)
		}
	}
}
