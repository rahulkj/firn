package apikeys

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/rahulkj/slipway/internal/log"
)

func TestAPIKeysRoundTrip(t *testing.T) {
	fl, err := log.Open(filepath.Join(t.TempDir(), "__api_keys-0"), log.WithBlockSealing(50*time.Millisecond))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s := NewStore(fl)

	key, err := s.Create("bench-user", "operator")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if key == "" || key == "ev-" {
		t.Fatal("create returned empty key")
	}

	// Verify resolves the key.
	k, ok := s.Verify(key)
	if !ok {
		t.Fatal("verify: expected known key")
	}
	if k.Name != "bench-user" || k.Role != "operator" {
		t.Fatalf("key payload = %+v", k)
	}
	if !s.Allow(key, RoleViewer) || !s.Allow(key, RoleOperator) {
		t.Fatal("allow check failed")
	}
	if s.Allow(key, RoleAdmin) {
		t.Fatal("operator key granted admin")
	}

	// List shows it.
	found := false
	for _, l := range s.List() {
		if l.Name == "bench-user" {
			found = true
		}
	}
	if !found {
		t.Fatal("list did not show the created key")
	}

	// Revoke blocks verify.
	if err := s.Revoke("bench-user"); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, ok := s.Verify(key); ok {
		t.Fatal("revoked key still verifies")
	}
}

func TestAPIKeysUnknownKeyRejected(t *testing.T) {
	s := NewStore(nil) // in-memory for this test
	if _, ok := s.Verify("ev-nope"); ok {
		t.Fatal("unknown key accepted")
	}
	if s.Allow("ev-nope", RoleViewer) {
		t.Fatal("unknown key allowed")
	}
}

func TestAPIKeysRoleRanking(t *testing.T) {
	s := NewStore(nil)
	// Nothing stored: all lookups fail.
	if _, ok := s.Verify("x"); ok {
		t.Fatal("empty store returned a key")
	}
}
