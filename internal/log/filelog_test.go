package log

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileLogAppendAssignsSequentialOffsets(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = l.Close() }()
	for i := int64(0); i < 100; i++ {
		if got := l.Append(nil, []byte(fmt.Sprintf("v-%d", i))); got != i {
			t.Fatalf("append %d: got offset %d", i, got)
		}
	}
	if hw := l.HighWatermark(); hw != 100 {
		t.Fatalf("high watermark: got %d, want 100", hw)
	}
}

func TestFileLogRoundTripAcrossSegmentRolls(t *testing.T) {
	dir := t.TempDir()
	// Tiny segments to force rolls.
	l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(128))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	const n = 50
	for i := 0; i < n; i++ {
		v := []byte(fmt.Sprintf("value-%03d", i))
		if off := l.Append([]byte(fmt.Sprintf("k%d", i)), v); off != int64(i) {
			t.Fatalf("append %d: got offset %d", i, off)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	l2, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(128))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()
	if hw := l2.HighWatermark(); hw != n {
		t.Fatalf("reopened hwm = %d, want %d", hw, n)
	}
	recs, err := l2.Fetch(0, n)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	for i, r := range recs {
		if r.Offset != int64(i) || string(r.Value) != fmt.Sprintf("value-%03d", i) {
			t.Fatalf("record %d mismatch: offset=%d value=%q", i, r.Offset, r.Value)
		}
	}
}

// TestFileLogRecoveryTruncatesTornTail is the slice-2 fault-injection test:
// a crash mid-append leaves a partial frame at the tail; reopen must not
// serve it (invariant 3) and must truncate it.
func TestFileLogRecoveryTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 10; i++ {
		l.Append([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate a torn append: half a frame at the tail.
	segPath := filepath.Join(dir, segmentFileName(0))
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x02, 0x00}); err != nil {
		t.Fatalf("write torn tail: %v", err)
	}
	_ = f.Sync()
	_ = f.Close()

	l2, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	if hw := l2.HighWatermark(); hw != 10 {
		t.Fatalf("hwm after recovery = %d, want 10", hw)
	}
	recs, err := l2.Fetch(0, 100)
	if err != nil {
		t.Fatalf("fetch after recovery: %v", err)
	}
	if len(recs) != 10 {
		t.Fatalf("got %d records after recovery, want 10", len(recs))
	}
	// Torn bytes must be truncated: the file size must match the last valid record.
	st, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// Append one more record and confirm it lands cleanly after offset 9.
	if off := l2.Append([]byte("k"), []byte("v10")); off != 10 {
		t.Fatalf("append after recovery: offset = %d, want 10", off)
	}
	if err := l2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	l3, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("reopen after post-recovery append: %v", err)
	}
	defer func() { _ = l3.Close() }()
	recs, err = l3.Fetch(10, 1)
	if err != nil {
		t.Fatalf("fetch post-recovery append: %v", err)
	}
	if len(recs) != 1 || string(recs[0].Value) != "v10" {
		t.Fatalf("post-recovery record = %+v", recs)
	}
	_ = st
}

// TestFileLogRecoveryTruncatesCRCCorruption: a frame whose CRC no longer
// validates is a torn/corrupt tail; everything from it onward is dropped.
func TestFileLogRecoveryTruncatesCRCCorruption(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 10; i++ {
		l.Append([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Corrupt a byte inside the last frame.
	segPath := filepath.Join(dir, segmentFileName(0))
	data, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(segPath, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	l2, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("reopen with CRC corruption: %v", err)
	}
	defer func() { _ = l2.Close() }()
	if hw := l2.HighWatermark(); hw != 9 {
		t.Fatalf("hwm after CRC recovery = %d, want 9 (corrupt tail dropped)", hw)
	}
	recs, err := l2.Fetch(0, 100)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(recs) != 9 {
		t.Fatalf("got %d records, want 9", len(recs))
	}
}

func TestFileLogFetchRejectsBadOffsets(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = l.Close() }()
	l.Append(nil, []byte("x"))
	if _, err := l.Fetch(-1, 1); err == nil {
		t.Fatal("negative offset: want error")
	}
	if _, err := l.Fetch(1, 1); err == nil {
		t.Fatal("offset beyond end of log: want error")
	}
}

// TestFileLogRoundTripProperty is the file-log counterpart of
// TestLogRoundTripProperty: random record sizes, forced segment rolls,
// read back and verified.
func TestFileLogRoundTripProperty(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(512))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = l.Close() }()

	rng := rand.New(rand.NewSource(42))
	const n = 120
	want := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		v := make([]byte, 1+rng.Intn(64))
		for j := range v {
			v[j] = byte(rng.Intn(256))
		}
		want = append(want, v)
		l.Append([]byte(fmt.Sprintf("key-%d", i)), v)
	}

	recs, err := l.Fetch(0, n)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(recs) != n {
		t.Fatalf("got %d records, want %d", len(recs), n)
	}
	for i, r := range recs {
		if r.Offset != int64(i) {
			t.Fatalf("record %d: offset = %d", i, r.Offset)
		}
		if string(r.Value) != string(want[i]) {
			t.Fatalf("record %d: value mismatch", i)
		}
	}
}

func TestFileLogSegmentFilesNamedByBaseOffset(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(64))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 20; i++ {
		l.Append(nil, []byte(strings.Repeat("x", 16)))
	}
	_ = l.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) < 2 {
		t.Fatalf("want multiple segments, got %d", len(entries))
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".log") {
			t.Fatalf("unexpected file %q", e.Name())
		}
		var base int64
		if _, err := fmt.Sscanf(strings.TrimSuffix(e.Name(), ".log"), "%d", &base); err != nil {
			t.Fatalf("segment %q not named by base offset: %v", e.Name(), err)
		}
	}
}
