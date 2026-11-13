package groups

import (
	"errors"
	"testing"

	"github.com/rahulkj/eventing/internal/fence"
)

func TestFencedOffsetStoreFailsClosed(t *testing.T) {
	s, err := NewOffsetStore("", nil)
	if err != nil {
		t.Fatalf("open offsets store: %v", err)
	}
	box := fence.NewGateBox()
	box.Set(func(string) error { return fence.ErrFenced })
	s.SetGate(box)

	err = s.Commit([]byte("g\x00orders\x000"), CommittedOffset{Offset: 5, Generation: 1})
	if !errors.Is(err, fence.ErrFenced) {
		t.Fatalf("fenced commit error = %v, want fence.ErrFenced", err)
	}
	if hw := s.inner.HighWatermark(); hw != 0 {
		t.Fatalf("HWM moved to %d on a fenced write", hw)
	}
	if _, ok := s.Get([]byte("g\x00orders\x000")); ok {
		t.Fatal("fenced write became visible to readers")
	}
	if err := s.Compact(); err == nil {
		t.Fatal("compact allowed on a fenced (replicated) internal log")
	}
}

func TestFencedOffsetStoreCrossWriteVisibleWithoutRestart(t *testing.T) {
	// Simulates what the replicator does on a follower: a frame is appended
	// to the shared FileLog behind the store's back. The read path must
	// notice via HWM movement with no restart and no manual refresh.
	dir := t.TempDir()
	s, err := NewOffsetStore(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s.inner.Append([]byte("g\x00orders\x000"), encodeCommittedOffset(CommittedOffset{Offset: 7, Generation: 1}))
	v, found := s.Get([]byte("g\x00orders\x000"))
	if !found || v.Offset != 7 {
		t.Fatalf("cross-node write not visible: found=%v offset=%d, want 7/true", found, v.Offset)
	}
}
