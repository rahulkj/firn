package queue

import (
	"errors"
	"testing"

	"github.com/rahulkj/slipway/internal/fence"
)

func TestFencedAttemptStoreFailsClosed(t *testing.T) {
	s, err := NewAttemptStore("", nil)
	if err != nil {
		t.Fatalf("open attempts store: %v", err)
	}
	box := fence.NewGateBox()
	box.Set(func(string) error { return fence.ErrFenced })
	s.SetGate(box)

	if err := s.Put("jobs", 0, 42, 1); !errors.Is(err, fence.ErrFenced) {
		t.Fatalf("fenced put error = %v, want fence.ErrFenced", err)
	}
	if hw := s.inner.HighWatermark(); hw != 0 {
		t.Fatalf("HWM moved to %d on a fenced write", hw)
	}
	if got := s.Attempts("jobs", 0, 42); got != 0 {
		t.Fatalf("fenced write visible: attempts=%d, want 0", got)
	}
	if err := s.Compact(); err == nil {
		t.Fatal("compact allowed on a fenced (replicated) internal log")
	}
}

func TestAttemptStoreCrossWriteVisibleWithoutRestart(t *testing.T) {
	// Same hazard class as __consumer_offsets: a frame appended behind the
	// store's back (the replicator on a follower) must become visible via
	// the HWM-gated fold, without restart.
	dir := t.TempDir()
	s, err := NewAttemptStore(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s.inner.Append(attemptKey("jobs", 0, 42), []byte{0, 0, 0, 3})
	if got := s.Attempts("jobs", 0, 42); got != 3 {
		t.Fatalf("cross-node write not visible: attempts=%d, want 3", got)
	}
}
