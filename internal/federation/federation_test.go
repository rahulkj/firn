package federation

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateShapeRejectsInternalNamespace(t *testing.T) {
	for _, name := range []string{"__consumer_offsets", "__federation_peers", "__federation_cursors", "__audit", "__queue_attempts", "__api_keys"} {
		err := ValidateShape(Peer{Name: name, Addr: "x", Direction: DirectionIn, Mode: ModePassive})
		if err == nil {
			t.Fatalf("expected error for reserved name %q", name)
		}
	}
}

func TestValidateShapeRejectsEmptyFields(t *testing.T) {
	if err := ValidateShape(Peer{Name: "", Addr: "x", Direction: DirectionIn, Mode: ModePassive}); err == nil {
		t.Fatal("empty name should error")
	}
	if err := ValidateShape(Peer{Name: "ok", Addr: "", Direction: DirectionIn, Mode: ModePassive}); err == nil {
		t.Fatal("empty addr should error")
	}
	if err := ValidateShape(Peer{Name: "ok", Addr: "n1:1", Direction: 9, Mode: ModePassive}); err == nil {
		t.Fatal("invalid direction should error")
	}
	if err := ValidateShape(Peer{Name: "ok", Addr: "n1:1", Direction: DirectionIn, Mode: 9}); err == nil {
		t.Fatal("invalid mode should error")
	}
	if err := ValidateShape(Peer{Name: "ok", Addr: "n1:1", Direction: DirectionIn, Mode: ModePassive,
		Selectors: []Selector{{Topics: []string{"t1", ""}}},
	}); err == nil {
		t.Fatal("empty topic in selector should error")
	}
}

func TestValidateShapeRequiresKeyForInbound(t *testing.T) {
	in := Peer{Name: "ok", Addr: "n:1", Direction: DirectionIn, Mode: ModePassive}
	if err := ValidateShape(in); err == nil {
		t.Fatal("direction in without key must error")
	}
	both := in
	both.Direction = DirectionBoth
	if err := ValidateShape(both); err == nil {
		t.Fatal("direction both without key must error")
	}
	out := Peer{Name: "ok", Addr: "n:1", Direction: DirectionOut, Mode: ModePassive}
	if err := ValidateShape(out); err != nil {
		t.Fatalf("direction out must not require a key: %v", err)
	}
	in.Key = "secret"
	if err := ValidateShape(in); err != nil {
		t.Fatalf("direction in with key must pass: %v", err)
	}
}

func TestValidateShapeRejectsInternalSelectors(t *testing.T) {
	p := Peer{Name: "ok", Addr: "n:1", Direction: DirectionIn, Mode: ModePassive, Key: "k",
		Selectors: []Selector{{Topics: []string{"__audit"}}}}
	if err := ValidateShape(p); err == nil {
		t.Fatal("internal topic in selector must error")
	}
	p = Peer{Name: "ok", Addr: "n:1", Direction: DirectionIn, Mode: ModePassive, Key: "k",
		Selectors: []Selector{{Queues: []string{"__consumer_offsets"}}}}
	if err := ValidateShape(p); err == nil {
		t.Fatal("internal name in queue selector must error")
	}
}

func TestParseDirectionAndMode(t *testing.T) {
	d, err := ParseDirection("in")
	if err != nil || d != DirectionIn {
		t.Fatalf("DirectionIn: %v", err)
	}
	d, err = ParseDirection("BOTH")
	if err != nil || d != DirectionBoth {
		t.Fatalf("DirectionBoth: %v", err)
	}
	_, err = ParseDirection("something")
	if err == nil {
		t.Fatal("bogus direction should error")
	}
	m, err := ParseMode("passive")
	if err != nil || m != ModePassive {
		t.Fatalf("ModePassive: %v", err)
	}
	m, err = ParseMode("active-active")
	if err != nil || m != ModeActiveActive {
		t.Fatalf("active-active: %v", err)
	}
	_, err = ParseMode("")
	if err == nil {
		t.Fatal("empty mode should error")
	}
}

func TestPeerAddListRemoveRoundTrip(t *testing.T) {
	s := OpenInMemory()
	if peers := s.ListPeers(); len(peers) != 0 {
		t.Fatalf("fresh store should have no peers, got %d", len(peers))
	}
	p := Peer{Name: "peer-a", Addr: "n1:9001", Direction: DirectionBoth, Mode: ModePassive, Key: "peer-key-abc",
		Selectors: []Selector{{Topics: []string{"orders"}, Queues: nil}}}
	if err := s.AddPeer(p); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	s2 := s.ListPeers()
	if len(s2) != 1 || s2[0].Name != "peer-a" {
		t.Fatalf("ListPeers: %+v", s2)
	}
	if s2[0].CreatedUnix == 0 || s2[0].UpdatedUnix == 0 {
		t.Fatalf("timestamps should be set: %+v", s2[0])
	}
	// Overwrite in place: CreatedUnix preserved, UpdatedUnix advanced.
	old := s2[0].CreatedUnix
	p.Mode = ModeActiveActive
	if err := s.AddPeer(p); err != nil {
		t.Fatalf("AddPeer overwrite: %v", err)
	}
	s3 := s.ListPeers()[0]
	if s3.CreatedUnix != old {
		t.Fatalf("CreatedUnix mutated on overwrite: %d -> %d", old, s3.CreatedUnix)
	}
	if s3.Mode != ModeActiveActive {
		t.Fatalf("Mode not updated: %v", s3.Mode)
	}
	if err := s.RemovePeer("peer-a"); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}
	if peers := s.ListPeers(); len(peers) != 0 {
		t.Fatalf("RemovePeer should drop the peer, got %d", len(peers))
	}
}

func TestCursorUpsertAndDelete(t *testing.T) {
	s := OpenInMemory()
	if cs := s.ListCursors(); len(cs) != 0 {
		t.Fatalf("fresh cursors: %v", cs)
	}
	c1 := Cursor{Peer: "p", Topic: "t", Partition: 0, LastSourceOffset: 100, LastLocalOffset: 100, Mode: ModePassive}
	if err := s.SetCursor(c1); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	cs := s.ListCursors()
	if len(cs) != 1 || cs[0].LastSourceOffset != 100 {
		t.Fatalf("cursor 1: %+v", cs)
	}
	c1.LastSourceOffset = 250
	if err := s.SetCursor(c1); err != nil {
		t.Fatalf("SetCursor update: %v", err)
	}
	cs = s.ListCursors()
	if len(cs) != 1 || cs[0].LastSourceOffset != 250 {
		t.Fatalf("cursor updated: %+v", cs)
	}
	// RemoveWholePeer drops it:
	if err := s.AddPeer(Peer{Name: "p", Addr: "n:1", Direction: DirectionIn, Mode: ModePassive, Key: "k"}); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	removed := s.RemoveWholePeer("p")
	if len(removed) != 1 {
		t.Fatalf("expected 1 cursor removed, got %v", removed)
	}
	if cs := s.ListCursors(); len(cs) != 0 {
		t.Fatalf("cursors should be empty after RemoveWholePeer, got %v", cs)
	}
}

func TestPeerPersistenceAcrossReload(t *testing.T) {
	dir := t.TempDir()
	s1, err := OpenFromDir(dir)
	if err != nil {
		t.Fatalf("OpenFromDir: %v", err)
	}
	p := Peer{Name: "peer-a", Addr: "n1:9001", Direction: DirectionIn, Mode: ModePassive, Key: "k"}
	if err := s1.AddPeer(p); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	// Re-open the same dir: peers should survive.
	s2, err := OpenFromDir(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, ok := s2.GetPeer("peer-a")
	if !ok {
		t.Fatal("peer not found after reload")
	}
	if got.Addr != "n1:9001" {
		t.Fatalf("addr mismatch after reload: %q", got.Addr)
	}
}

func TestDeleteMarkerInFold(t *testing.T) {
	s := OpenInMemory()
	p := Peer{Name: "x", Addr: "n:1", Direction: DirectionIn, Mode: ModePassive, Key: "k"}
	if err := s.AddPeer(p); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	if err := s.RemovePeer("x"); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}
	// Write a garbage delete marker directly; fold must drop the peer.
	s.peers.Append([]byte("peer:x"), []byte(`{"op":"delete"}`))
	if err := s.loadLocked(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if _, ok := s.GetPeer("x"); ok {
		t.Fatal("delete marker was not honoured on reload")
	}
}

var _ = errors.Is
var _ = strings.TrimSpace
