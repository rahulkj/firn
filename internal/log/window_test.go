package log

// Property tests for ADR-014 (group commit / flush window). These were
// written before the implementation exists; they pin the durability
// contract:
//
//   1. ack ⇒ durable: after Append returns, the record is on disk.
//   2. no ack ⇒ no visibility: staged-but-unflushed frames are never
//      returned by Fetch and never counted by HighWatermark.
//   3. atomic window: a flush makes the whole staged batch durable in one
//      fsync; a fetch concurrent with the flush never sees a partial batch.
//   4. monotonic allocation at flush time: offsets are 0..N-1, no gaps,
//      across windows and across segment rolls.
//   5. rollback on fsync failure: the whole window is rolled back; no
//      unacked frame survives; the log resumes cleanly afterwards.
//
// Whitebox tests (same package) — they reach the pending-window state
// directly, which is the only way to observe "not yet flushed".

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestFlushWindowAppendWaitsForFlush: an Append in window mode must not
// return until the window holding its frame is durable.
func TestFlushWindowAppendWaitsForFlush(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithFlushWindow(150*time.Millisecond), WithMaxBatchBytes(1<<20))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = l.Close() }()
	start := time.Now()
	if off := l.Append(nil, []byte("hello")); off != 0 {
		t.Fatalf("offset = %d, want 0", off)
	}
	if elapsed := time.Since(start); elapsed < 120*time.Millisecond {
		t.Fatalf("Append returned after %v; expected to block until the flush window (~150ms)", elapsed)
	}
}

// TestFlushWindowAckIsDurable: after Append returns, a fresh open of the
// log (new handle, real process-level durability semantics) sees the record.
func TestFlushWindowAckIsDurable(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithFlushWindow(50*time.Millisecond))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if off := l.Append([]byte("k"), []byte("payload")); off != 0 {
		t.Fatalf("offset = %d", off)
	}
	// No clean Close — simulate a crash by abandoning the handle without a
	// final Sync (Close would flush; here we want to prove the record was
	// durable without it).
	_ = l
	l2, err := Open(dir, WithSyncOnAppend(false), WithFlushWindow(50*time.Millisecond))
	if err != nil {
		t.Fatalf("reopen without close: %v", err)
	}
	defer func() { _ = l2.Close() }()
	if hw := l2.HighWatermark(); hw != 1 {
		t.Fatalf("reopened high watermark = %d, want 1", hw)
	}
	recs, err := l2.Fetch(0, 1)
	if err != nil || len(recs) != 1 || string(recs[0].Value) != "payload" {
		t.Fatalf("reopened fetch: recs=%v err=%v", recs, err)
	}
}

// TestFlushWindowNoAckNoVisibility: staged-but-unflushed frames are invisible.
func TestFlushWindowNoAckNoVisibility(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithFlushWindow(5*time.Second), WithMaxBatchBytes(1<<20))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = l.Close() }()

	done := make(chan struct{})
	go func() { defer close(done); _ = l.Append(nil, []byte("staged")) }()
	time.Sleep(30 * time.Millisecond) // let the frame stage

	if hw := l.HighWatermark(); hw != 0 {
		t.Fatalf("high watermark = %d while a frame is staged-but-unflushed, want 0", hw)
	}
	for i := 0; i < 3; i++ {
		recs, err := l.Fetch(0, 10)
		if err == nil && len(recs) != 0 {
			t.Fatalf("fetch before flush returned %d records, want none", len(recs))
		}
		time.Sleep(10 * time.Millisecond)
	}
	<-done
}

// TestFlushWindowFetchNeverSeesPartialWindow: a fetch concurrent with a
// flush must never observe a half-written window — not even while frames
// are being written to the segment.
func TestFlushWindowFetchNeverSeesPartialWindow(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithFlushWindow(10*time.Second), WithMaxBatchBytes(1<<20))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = l.Close() }()

	// Whitebox hook: runs for every frame written during a flush. Must not
	// take l.mu (the flush holds it) — hence fetchLocked.
	seenDuringFlush := 0
	l.flushWriteHook = func(_ int64) {
		recs, err := l.fetchLocked(0, 100)
		if err == nil {
			seenDuringFlush += len(recs) // must stay 0 until the flush's final Sync
		}
	}

	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = l.Append(nil, []byte(fmt.Sprintf("v-%03d", i)))
		}(i)
	}
	// Wait until all frames are staged, then flush by hand (whitebox) and
	// let the waiters complete.
	for {
		l.mu.Lock()
		pending := len(l.pendingFramesLocked())
		l.mu.Unlock()
		if pending == n {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	l.flush()
	wg.Wait()

	if seenDuringFlush != 0 {
		t.Fatalf("fetch during flush observed %d records of the in-flight window, want 0", seenDuringFlush)
	}
	recs, err := l.Fetch(0, n)
	if err != nil || len(recs) != n {
		t.Fatalf("after flush: fetched %d records (err=%v), want %d", len(recs), err, n)
	}
}

// TestFlushWindowOffsetsContiguousAcrossWindowsAndRolls: offsets are
// 0..N-1, no gaps, across multiple windows and segment rolls.
func TestFlushWindowOffsetsContiguousAcrossWindowsAndRolls(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false),
		WithFlushWindow(30*time.Millisecond), WithMaxBatchBytes(512), WithMaxSegmentBytes(600))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	const n = 60
	value := bytes.Repeat([]byte{0xAB}, 96)
	for i := 0; i < n; i++ {
		if off := l.Append(nil, value); off != int64(i) {
			t.Fatalf("append %d: offset = %d, want %d", i, off, i)
		}
	}
	// Fetch must return all n, contiguous.
	recs, err := l.Fetch(0, n)
	if err != nil || len(recs) != n {
		t.Fatalf("fetched %d (err=%v), want %d", len(recs), err, n)
	}
	for i, r := range recs {
		if r.Offset != int64(i) {
			t.Fatalf("record %d has offset %d", i, r.Offset)
		}
	}
	if hw := l.HighWatermark(); hw != n {
		t.Fatalf("high watermark = %d, want %d", hw, n)
	}
	_ = l.Close()
	// Reopen and confirm the same layout persists.
	l2, err := Open(dir, WithSyncOnAppend(false), WithFlushWindow(30*time.Millisecond))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()
	if hw := l2.HighWatermark(); hw != n {
		t.Fatalf("reopened high watermark = %d, want %d", hw, n)
	}
}

// TestFlushWindowCloseDrainsPending: Close flushes whatever is staged.
func TestFlushWindowCloseDrainsPending(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithFlushWindow(10*time.Second))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	done := make(chan int64, 1)
	go func() { done <- l.Append(nil, []byte("worth surviving")) }()
	time.Sleep(20 * time.Millisecond) // let the frame stage
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if off := <-done; off != 0 {
		t.Fatalf("staged append unblocked with offset %d, want 0 (Close must flush pending)", off)
	}
	l2, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()
	if hw := l2.HighWatermark(); hw != 1 {
		t.Fatalf("reopened high watermark = %d, want 1 (Close must flush pending)", hw)
	}
}

// TestFlushWindowRollbackOnFsyncFailure: when the flush's fsync fails, the
// entire window is rolled back — nothing unacked survives, the log resumes
// cleanly, and reopening sees zero of the failed batch.
func TestFlushWindowRollbackOnFsyncFailure(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, WithSyncOnAppend(false), WithFlushWindow(50*time.Millisecond))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	l.injectedSyncErr = fmt.Errorf("injected fsync failure") // non-nil: all syncs fail until cleared
	// Three appends share one window; all three must fail.
	panics := 0
	for i := 0; i < 3; i++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					panics++
				}
			}()
			_ = l.Append(nil, []byte(fmt.Sprintf("lost-%d", i)))
		}()
	}
	if panics != 3 {
		t.Fatalf("appends panicked %d times, want 3 (whole window fails)", panics)
	}
	if hw := l.HighWatermark(); hw != 0 {
		t.Fatalf("high watermark = %d after rollback, want 0", hw)
	}
	l.injectedSyncErr = nil
	if off := l.Append(nil, []byte("kept")); off != 0 {
		t.Fatalf("append after failure got offset %d, want 0 (rollback burned failed offsets)", off)
	}
	_ = l.Close()
	l2, err := Open(dir, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()
	recs, err := l2.Fetch(0, 10)
	if err != nil || len(recs) != 1 || string(recs[0].Value) != "kept" {
		t.Fatalf("reopened records = %v (err=%v), want only 'kept'", recs, err)
	}
}
