package log

import (
	"fmt"
	"os"
	"testing"
)

// TestFileLogCompactKeepsLastPerKey: fold semantics — superseded keys are
// dropped; the last value per key survives; offsets are renumbered densely;
// high watermark = number of surviving records.
func TestFileLogCompactKeepsLastPerKey(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	keys := []string{"a", "b", "a", "c", "b", "a"}
	for i, k := range keys {
		l.Append([]byte(k), []byte(fmt.Sprintf("v%d", i)))
	}
	if err := l.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if hw := l.HighWatermark(); hw != 3 {
		t.Fatalf("hwm after compact = %d, want 3", hw)
	}
	recs, err := l.Fetch(0, 10)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	byKey := map[string]string{}
	for i, r := range recs {
		if r.Offset != int64(i) {
			t.Fatalf("record %d: offset = %d (offsets must be dense from 0)", i, r.Offset)
		}
		byKey[string(r.Key)] = string(r.Value)
	}
	want := map[string]string{"a": "v5", "b": "v4", "c": "v3"}
	for k, v := range want {
		if byKey[k] != v {
			t.Fatalf("key %q = %q, want %q", k, byKey[k], v)
		}
	}

	// Reopen: compacted state must survive.
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	l2, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("reopen compacted log: %v", err)
	}
	defer func() { _ = l2.Close() }()
	if hw := l2.HighWatermark(); hw != 3 {
		t.Fatalf("reopened hwm = %d, want 3", hw)
	}
	recs, err = l2.Fetch(0, 10)
	if err != nil {
		t.Fatalf("fetch reopened: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d records after reopen, want 3", len(recs))
	}
}

// TestFileLogCompactDenseOffsetsInOrder: surviving records are ordered by
// their original offsets, renumbered 0..M-1 without gaps.
func TestFileLogCompactDenseOffsetsInOrder(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	seq := []string{"x", "y", "x", "x", "y", "z"}
	for i, k := range seq {
		l.Append([]byte(k), []byte{byte('a' + i)})
	}
	if err := l.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	recs, err := l.Fetch(0, 10)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	gotKeys := []string{}
	for i, r := range recs {
		if r.Offset != int64(i) {
			t.Fatalf("record %d: offset = %d (offsets must be dense from 0)", i, r.Offset)
		}
		gotKeys = append(gotKeys, string(r.Key))
	}
	if fmt.Sprint(gotKeys) != "[x y z]" {
		t.Fatalf("survivors = %v, want [x y z]", gotKeys)
	}
}

// TestFileLogCompactEmptyLog: compacting an empty log is a no-op.
func TestFileLogCompactEmptyLog(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := l.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if hw := l.HighWatermark(); hw != 0 {
		t.Fatalf("hwm = %d, want 0", hw)
	}
}

// TestFileLogCompactCrashRecovery: crash windows leave .compact/.old behind.
// Open must recover: .compact preferred when the live dir is missing,
// otherwise .old is restored.
func TestFileLogCompactCrashRecovery(t *testing.T) {
	mktLog := func(t *testing.T, dir string) {
		t.Helper()
		l, err := Open(dir, WithSyncOnAppend(false))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		l.Append([]byte("k"), []byte("v"))
		if err := l.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	// Only .old present (crash after renaming dir -> .old, before/during
	// writing .compact): restore from .old.
	dir := t.TempDir()
	dir2 := dir + "/log"
	mktLog(t, dir2)
	if err := os.Rename(dir2, dir2+".old"); err != nil {
		t.Fatalf("rename to .old: %v", err)
	}
	l2, err := Open(dir2, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open with only .old present: %v", err)
	}
	if hw := l2.HighWatermark(); hw != 1 {
		t.Fatalf("hwm = %d, want 1 (restored from .old)", hw)
	}
	_ = l2.Close()

	// Only .compact present (crash after renaming .compact -> dir target
	// didn't complete... i.e. .compact holds the only copy): prefer .compact.
	dir3 := dir + "/log2"
	mktLog(t, dir3)
	if err := os.Rename(dir3, dir3+".compact"); err != nil {
		t.Fatalf("rename to .compact: %v", err)
	}
	l3, err := Open(dir3, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open with only .compact present: %v", err)
	}
	if hw := l3.HighWatermark(); hw != 1 {
		t.Fatalf("hwm = %d, want 1 (recovered from .compact)", hw)
	}
	_ = l3.Close()
}

// TestFileLogCompactThenAppend: after compaction, new appends continue with
// dense offsets and no conflicts with the renumbered state.
func TestFileLogCompactThenAppend(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	l.Append([]byte("a"), []byte("1"))
	l.Append([]byte("b"), []byte("1"))
	l.Append([]byte("a"), []byte("2"))
	if err := l.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	off := l.Append([]byte("c"), []byte("3"))
	if off != 2 { // a and b survive compaction (dense 0..1)
		t.Fatalf("post-compact append offset = %d, want 2", off)
	}
	recs, err := l.Fetch(0, 10)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(recs) != 3 || string(recs[2].Key) != "c" {
		t.Fatalf("records = %+v", recs)
	}
}
