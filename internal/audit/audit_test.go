package audit

import (
	"errors"
	"testing"

	"github.com/rahulkj/firn/internal/fence"
	"github.com/rahulkj/firn/internal/log"
)

func TestAppendWithMemStoreCountsBytes(t *testing.T) {
	s := NewMemStore(0)
	before := s.BytesWritten()
	if err := s.Append("who", "action", "target", "result"); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := s.BytesWritten(); got <= before {
		t.Fatalf("bytes written = %d, want > %d", got, before)
	}
	if s.BudgetExceeded() {
		t.Fatal("budget must not be exceeded with unlimited budget")
	}
}

func TestBudgetExhaustionIsFailClosed(t *testing.T) {
	// Budget tiny enough that one smallest-record append fits but the next doesn't.
	s := NewMemStore(1) // 1 byte — no record fits
	err := s.Append("w", "a", "t", "r")
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("want ErrBudgetExhausted, got %v", err)
	}
	if !s.BudgetExceeded() {
		t.Fatal("BudgetExceeded must be true after exhaustion")
	}
	// Once exhausted, stays exhausted.
	if err := s.Append("w", "a", "t", "r"); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("still want ErrBudgetExhausted, got %v", err)
	}
}

func TestBudgetMidpointLetsExactlyOneComplete(t *testing.T) {
	// First append under a generous budget succeeds; then force exhaustion with a following tiny budget store.
	s := NewMemStore(0)
	for i := 0; i < 20; i++ {
		if err := s.Append("w", "a", "t", "r"); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if s.BudgetExceeded() {
		t.Fatal("budget should not exhaust with unlimited budget")
	}
}

func TestNilStoreAndSilentAuditFallThrough(t *testing.T) {
	// A nil *Store must act as a silent noop (not a panic).
	if (*Store)(nil) != nil {
		t.Fatal("nil store sanity")
	}
	if err := (*Store)(nil).Append("w", "a", "t", "r"); err != nil {
		t.Fatalf("nil store Append must not error, got %v", err)
	}
	// BudgetExceeded on nil store reports false.
	if (*Store)(nil).BudgetExceeded() {
		t.Fatal("nil store BudgetExceeded must be false")
	}
	if got := (*Store)(nil).BytesWritten(); got != 0 {
		t.Fatalf("nil store BytesWritten = %d, want 0", got)
	}
}

func TestMustAppendPanicsOnBudgetExhaustion(t *testing.T) {
	s := NewMemStore(1) // nothing fits
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from MustAppend when budget cannot hold a record")
		}
	}()
	s.MustAppend("w", "a", "t", "r")
}

func TestFenceRefusalRejectsWrite(t *testing.T) {
	gb := fence.NewGateBox()
	gb.Set(func(topic string) error {
		if topic == "__audit" {
			return fence.ErrFenced // refused on this node
		}
		return nil
	})

	dir := t.TempDir()
	fl, err := log.Open(dir+"/__audit-0", log.WithBlockSealing(0))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = fl.Close() }()
	s := NewStore(fl, 0)
	s.SetGate(gb)
	err = s.Append("w", "a", "t", "r")
	if !errors.Is(err, fence.ErrFenced) {
		t.Fatalf("want fenced error, got %v", err)
	}
}
