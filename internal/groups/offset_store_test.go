package groups

// Acceptance tests for plan 006 slice 3: the __consumer_offsets commit path
// must stop re-folding the whole log on every local commit — O(N²) total.
// The store only ever needs to re-fold when the log moved underneath it
// without its knowledge (a cross-node replicated apply to the shared log) —
// local commits already know exactly what they appended.
//
// Red before the fix: Commit did not advance foldedHWM, so every Get after
// a commit observed "movement" and re-folded the entire log.

import (
	"path/filepath"
	"testing"

	"github.com/rahulkj/firn/internal/meta"
)

// externalAppend simulates a replicated write this store did not perform
// (the broker's shared internal-topic handle is applied by the replication
// agent on every node). Whitebox: appends straight into the store's log.
func externalAppend(t *testing.T, s *OffsetStore, key []byte, o CommittedOffset) {
	t.Helper()
	s.inner.Append(key, encodeCommittedOffset(o))
}

// TestCommitDoesNotRefoldPerCommit: N sequential commits, zero full folds.
func TestCommitDoesNotRefoldPerCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "__consumer_offsets-0")
	s, err := NewOffsetStore(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	const n = 500
	for i := 0; i < n; i++ {
		if err := s.Commit(CommitKey("g", "t", 0), CommittedOffset{Offset: int64(i + 1), Generation: 1, Leader: "x"}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	if s.folds != 0 {
		t.Fatalf("%d full re-folds over %d local commits; want 0 (the O(N²) bug)", s.folds, n)
	}
	if hw := s.inner.HighWatermark(); s.foldedHWM != hw {
		t.Fatalf("foldedHWM = %d, want %d after local commits", s.foldedHWM, hw)
	}
	if got, found := s.Get(CommitKey("g", "t", 0)); !found || got.Offset != n {
		t.Fatalf("get after %d commits: offset=%d found=%v, want %d", n, got.Offset, found, n)
	}
	if s.folds != 0 {
		t.Fatalf("Get triggered %d re-folds after local commits; want 0", s.folds)
	}
}

// TestExternalWriteStillRefoldsOnRead: a write this store did not perform
// must re-fold exactly once on the next read — read-through for
// cross-node visibility must survive the slice-3 fix.
func TestExternalWriteStillRefoldsOnRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "__consumer_offsets-0")
	s, err := NewOffsetStore(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Commit(CommitKey("g", "a", 0), CommittedOffset{Offset: 7, Generation: 1, Leader: "a"}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if s.folds != 0 {
		t.Fatalf("local commit re-folded; want 0")
	}

	externalAppend(t, s, CommitKey("g", "b", 0), CommittedOffset{Offset: 42, Generation: 1, Leader: "b"})
	if s.folds != 0 {
		t.Fatalf("external append re-folded; want the read to do it")
	}
	if got, found := s.Get(CommitKey("g", "b", 0)); !found || got.Offset != 42 {
		t.Fatalf("external record invisible: offset=%d found=%v, want 42 — read-through broke", got.Offset, found)
	}
	if s.folds != 1 {
		t.Fatalf("fold count = %d for one external write, want exactly 1", s.folds)
	}
	// The local record must be intact after the external re-fold.
	if got, found := s.Get(CommitKey("g", "a", 0)); !found || got.Offset != 7 {
		t.Fatalf("local record lost after external re-fold: offset=%d found=%v, want 7", got.Offset, found)
	}
}

// TestCommitBelowReplicatedOffsetRejected: a replicated apply between local
// commits must be observed by the coordinator's regression check before it
// accepts another commit. One catch-up fold, never zero, never more.
func TestCommitBelowReplicatedOffsetRejected(t *testing.T) {
	dir := t.TempDir()
	reg := meta.NewRegistry()
	if err := reg.Create("events", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	store, err := NewOffsetStore(dir, nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer func() { _ = store.Close() }()
	c, err := NewCoordinator(reg, store)
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	if err := c.CommitOffset("g", "events", 0, 10, 0, false); err != nil {
		t.Fatalf("commit 10: %v", err)
	}
	// A replicated apply (offset 99) lands in the shared internal log
	// between local commits — exactly the once-in-a-while read-through case.
	externalAppend(t, store, CommitKey("g", "events", 0), CommittedOffset{Offset: 99, Generation: 1, Leader: "b"})
	if err := c.CommitOffset("g", "events", 0, 12, 0, false); err == nil {
		t.Fatal("commit below replicated offset must be rejected")
	}
	if store.folds != 1 {
		t.Fatalf("fold count = %d, want exactly 1 (the single catch-up fold)", store.folds)
	}
}

// TestFoldEquivalenceToFullRefold: incremental-apply state must be identical
// to a fresh full fold (read through close+reopen, which refolds by
// construction).
func TestFoldEquivalenceToFullRefold(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "__consumer_offsets-0")
	s, err := NewOffsetStore(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	committed := map[int]CommittedOffset{}
	for i := 0; i < 300; i++ {
		p := i % 3
		committed[p] = CommittedOffset{Offset: int64(i + 1), Generation: int64(i % 5), Leader: "m"}
		if err := s.Commit(CommitKey("g", "t", int32(p)), committed[p]); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	_ = s.Close()
	s2, err := NewOffsetStore(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()
	all := s2.All()
	if len(all) != 3 {
		t.Fatalf("folded %d keys, want 3", len(all))
	}
	for p := 0; p < 3; p++ {
		want := committed[p]
		got, found := all[string(CommitKey("g", "t", int32(p)))]
		if !found {
			t.Fatalf("partition %d missing after reopen", p)
		}
		if got.Offset != want.Offset || got.Generation != want.Generation {
			t.Fatalf("partition %d = %+v, want %+v", p, got, want)
		}
	}
}

// BenchmarkCommitOffsetScaling guards the O(N) property of the commit
// path: cost per commit must not grow with the log's length.
func BenchmarkCommitOffsetScaling(b *testing.B) {
	path := filepath.Join(b.TempDir(), "__consumer_offsets-0")
	s, err := NewOffsetStore(path, nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()
	for i := 0; i < b.N; i++ {
		if err := s.Commit(CommitKey("g", "t", int32(i%3)), CommittedOffset{Offset: int64(i + 1), Generation: 1, Leader: "x"}); err != nil {
			b.Fatal(err)
		}
	}
}
