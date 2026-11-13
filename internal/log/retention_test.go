package log

import (
	"os"
	"strings"
	"testing"
	"time"
)

// appendAt is a test-only hook to control record timestamps for retention.
func appendAt(t *testing.T, l *FileLog, ts time.Time, key, value []byte) int64 {
	t.Helper()
	return l.appendWithTimestamp(key, value, ts.UnixMilli())
}

func TestFileLogRetainOlderThanDeletesWholeSegmentsOnly(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(256))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	base := time.Now().Add(-2 * time.Hour)
	payload := []byte(strings.Repeat("v", 32))
	for i := 0; i < 30; i++ {
		appendAt(t, l, base.Add(time.Duration(i)*time.Minute), []byte("k"), payload)
	}
	hw := l.HighWatermark()

	deleted, err := l.RetainOlderThan(time.Now().Add(-3 * time.Hour)) // nothing older than 3h
	if err != nil {
		t.Fatalf("retain nothing: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted %d segments, want 0", deleted)
	}

	deleted, err = l.RetainOlderThan(base.Add(15 * time.Minute)) // older than minute 15
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if deleted == 0 {
		t.Fatal("expected whole segments to be deleted")
	}
	if hw != l.HighWatermark() {
		t.Fatal("retention must not move the high watermark")
	}
	// The active segment must never be deleted.
	if len(l.segments) == 0 {
		t.Fatal("no segments left")
	}

	// Fetching a deleted offset must error; fetching from the horizon works.
	first := l.segments[0].baseOffset
	if first == 0 {
		t.Fatal("expected oldest segments to be deleted")
	}
	if _, err := l.Fetch(0, 1); err == nil {
		t.Fatal("fetch before retention horizon: want error")
	}
	recs, err := l.Fetch(first, 1)
	if err != nil {
		t.Fatalf("fetch at horizon: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d records at horizon, want 1", len(recs))
	}
}

func TestFileLogRetainOlderThanNeverDeletesActiveSegment(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Single record, very old: the only (active) segment must survive.
	appendAt(t, l, time.Now().Add(-48*time.Hour), []byte("k"), []byte("old"))
	deleted, err := l.RetainOlderThan(time.Now())
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if deleted != 0 {
		t.Fatal("deleted the active segment")
	}
	recs, err := l.Fetch(0, 1)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(recs) != 1 {
		t.Fatal("active segment was deleted")
	}
}

func TestFileLogRetainOverBytesDeletesOldestFirst(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(256))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	payload := []byte(strings.Repeat("v", 32))
	for i := 0; i < 30; i++ {
		l.Append([]byte("k"), payload)
	}

	var sizeBefore int64
	for _, s := range l.segments {
		sz, err := segmentSize(s)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		sizeBefore += sz
	}
	budget := sizeBefore / 2
	deleted, err := l.RetainOverBytes(budget)
	if err != nil {
		t.Fatalf("retain over bytes: %v", err)
	}
	if deleted == 0 {
		t.Fatal("expected segments to be deleted")
	}
	// The active segment must survive even if it exceeds the budget.
	// High watermark must not move.
	if l.HighWatermark() != 30 {
		t.Fatalf("hwm = %d, want 30", l.HighWatermark())
	}
	// Only a prefix of segments may be deleted; the remainder starts at a
	// base offset > 0 and is contiguous.
	first := l.segments[0].baseOffset
	if first == 0 {
		t.Fatal("expected oldest segments to be deleted")
	}
	if _, err := l.Fetch(0, 1); err == nil {
		t.Fatal("fetch before horizon: want error")
	}
	recs, err := l.Fetch(first, 1000)
	if err != nil {
		t.Fatalf("fetch at horizon: %v", err)
	}
	for i, r := range recs {
		if r.Offset != recs[0].Offset+int64(i) {
			t.Fatalf("record %d: non-contiguous offset %d", i, r.Offset)
		}
	}
}

func TestFileLogRetainOverBytesZeroMeansDisabled(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	l.Append(nil, []byte("x"))
	deleted, err := l.RetainOverBytes(0)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if deleted != 0 {
		t.Fatal("budget 0 must disable byte-based retention")
	}
}

func TestFileLogFetchAtHorizonBoundary(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(256))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	payload := []byte(strings.Repeat("v", 32))
	for i := 0; i < 20; i++ {
		l.Append([]byte("k"), payload)
	}
	if _, err := l.RetainOlderThan(time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("retain: %v", err)
	}
	// Contract: offset < horizon => error; offset >= hwm => error
	// (MemLog parity); offset == horizon => ok.
	first := l.RetentionHorizon()
	hw := l.HighWatermark()
	if _, err := l.Fetch(first-1, 1); err == nil {
		t.Fatal("offset below horizon: want error")
	}
	if _, err := l.Fetch(hw, 1); err == nil {
		t.Fatal("offset at hwm: want error (MemLog parity)")
	}
	if _, err := l.Fetch(first, 1); err != nil {
		t.Fatalf("fetch at horizon: %v", err)
	}
}

func TestFileLogSegmentsOnDiskAfterRetain(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(128))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	payload := []byte(strings.Repeat("v", 16))
	for i := 0; i < 20; i++ {
		l.Append(nil, payload)
	}
	hwm := l.HighWatermark()
	if _, err := l.RetainOverBytes(64); err != nil {
		t.Fatalf("retain: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: recovery must succeed after retention (scan from the first
	// remaining segment; offsets stay contiguous in the middle).
	l2, err := Open(dir, WithSyncOnAppend(false), WithMaxSegmentBytes(128))
	if err != nil {
		t.Fatalf("reopen after retention: %v", err)
	}
	defer func() { _ = l2.Close() }()
	if l2.HighWatermark() != hwm {
		t.Fatalf("hwm after reopen = %d, want %d", l2.HighWatermark(), hwm)
	}
	first := l2.RetentionHorizon()
	recs, err := l2.Fetch(first, int(hwm-first))
	if err != nil {
		t.Fatalf("fetch after reopen: %v", err)
	}
	for i, r := range recs {
		if r.Offset != first+int64(i) {
			t.Fatalf("record %d: offset = %d, want %d", i, r.Offset, first+int64(i))
		}
	}
	// Every remaining segment file must exist on disk.
	for _, s := range l2.segments {
		if _, err := os.Stat(s.path); err != nil {
			t.Fatalf("segment missing: %v", err)
		}
	}
}
