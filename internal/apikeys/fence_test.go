package apikeys

import (
	"errors"
	"testing"

	"github.com/rahulkj/firn/internal/fence"
)

func TestFencedAPIKeysStoreFailsClosed(t *testing.T) {
	s := NewStore(nil) // in-memory store is fine; the fence is what we test
	box := fence.NewGateBox()
	box.Set(func(string) error { return fence.ErrFenced })
	s.SetGate(box)

	if _, err := s.Create("smoke", "admin"); !errors.Is(err, fence.ErrFenced) {
		t.Fatalf("fenced create error = %v, want fence.ErrFenced", err)
	}
	if err := s.Revoke("smoke"); !errors.Is(err, fence.ErrFenced) {
		t.Fatalf("fenced revoke error = %v, want fence.ErrFenced", err)
	}
	// Reads stay open on every node.
	if _, ok := s.Verify("ev-anything"); ok {
		t.Fatal("unknown key verified on a fenced store")
	}
	if keys := s.List(); len(keys) != 0 {
		t.Fatalf("fenced create leaked into List: %d keys", len(keys))
	}
	if s.HasAnyKey() {
		t.Fatal("HasAnyKey true on a fenced store that never wrote")
	}
}
