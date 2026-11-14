package log

// Slice 2 (docs/plans/001-mvp-single-node.md): segmented, file-backed log
// with CRC-validated frames and torn-tail truncation on open, per
// docs/adr/003-storage.md.
//
// On-disk frame layout (extends the ADR-003 sketch with explicit key/value
// lengths so frames are self-describing):
//
//	[len u32][crc u32][offset i64][ts i64][keyLen u32][valueLen u32][key][value]
//
// len = number of bytes after the len field (crc through value).
// crc = crc32.ChecksumIEEE over everything after the crc field.
//
// Semantics match MemLog exactly: offsets are assigned monotonically from 0,
// Fetch rejects offset < 0 and offset >= high watermark, and fetch never
// returns records at or above the high watermark.
//
// FileLog serializes its own access with an internal mutex (leaf lock):
// exported methods may be called concurrently from multiple goroutines.
// The broker, group coordinator, and replication agent share the internal
// logs concurrently; the mutex is what guarantees their safety. fsync runs
// with the lock held (correctness over tail latency on this path).
//
// Crash recovery on open: each segment is scanned frame by frame; the first
// frame that is incomplete, fails CRC, or carries an unexpected offset marks
// the start of a torn tail. A torn tail in the final segment is truncated;
// corruption in any earlier segment is a hard error (deleting committed data
// silently is never acceptable).

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	segmentSuffix = ".log"

	defaultMaxSegmentBytes = 64 << 20 // 64 MiB

	// sparseIndexInterval is how often (in records) a position is indexed.
	sparseIndexInterval = 64
)

// Option configures a FileLog.
type Option func(*FileLog)

// FsyncObserver, if set, is called with the duration of every fsync FileLog
// performs (append path, segment roll, Flush, close). It runs on the append
// path: implementations must be fast and must not call back into the log.
// The metrics package registers it at process start (ADR-006).
var FsyncObserver func(time.Duration)

func observeFsync(start time.Time) {
	if FsyncObserver != nil {
		FsyncObserver(time.Since(start))
	}
}

// WithMaxSegmentBytes sets the segment roll threshold.
func WithMaxSegmentBytes(n int64) Option {
	return func(l *FileLog) {
		if n > 0 {
			l.maxSegmentBytes = n
		}
	}
}

// WithSyncOnAppend controls whether every Append is followed by fsync.
// Defaults to true: an acknowledged produce must survive kill -9 (ADR-004,
// acks=1). Tests that don't exercise durability may disable it for speed.
func WithSyncOnAppend(on bool) Option {
	return func(l *FileLog) { l.syncOnAppend = on }
}

// WithBlockSealing asks for the active block to be sealed (rolled to a new
// segment) once it has lived for at least d, even if it hasn't filled.
// ADR-007 phase A: blocks are derived from segments; sealing early bounds
// the unsealed window. 0 (default) seals only on size.
func WithBlockSealing(d time.Duration) Option {
	return func(l *FileLog) { l.sealAge = d }
}

// WithFlushWindow switches the append path to group commit (ADR-014):
// Append stages the frame in memory and returns only after the window
// holding it has been written to the segment and fsynced. d must be > 0.
// Default (flushWindow == 0) keeps the v0.4.0 fsync-per-append behavior.
func WithFlushWindow(d time.Duration) Option {
	return func(l *FileLog) {
		if d > 0 {
			l.flushWindow = d
		}
	}
}

// WithMaxBatchBytes bounds the staged-but-unflushed bytes per partition
// window. Staging is byte-bounded; steady-state staged size ~= maxBatchBytes.
// Ignored when WithFlushWindow is not set. Default 1 MiB.
func WithMaxBatchBytes(n int64) Option {
	return func(l *FileLog) {
		if n > 0 {
			l.maxBatchBytes = n
		}
	}
}

// FileLog is an append-only, segmented log stored in a directory. Segments
// are named <baseOffset:020d>.log; the highest-numbered segment is active.
type FileLog struct {
	dir             string
	maxSegmentBytes int64
	syncOnAppend    bool
	sealAge         time.Duration

	// ADR-014 (flush window). Only active when flushWindow > 0.
	flushWindow   time.Duration
	maxBatchBytes int64
	pending       []pendingFrame // staged, unwritten, unflushed
	stagingBytes  int64
	flushTimer    *time.Timer

	// Test-only hooks. Untouched in production builds (always nil).
	flushWriteHook  func(off int64) // called per frame written during a flush
	injectedSyncErr error           // non-nil: the batch fsync fails (fault injection)

	// flushPoisoned latches true if a flush rollback ever fails; appends are
	// refused until restart. Fail-closed by design.
	flushPoisoned bool

	mu         sync.Mutex
	segments   []*segment
	hwm        int64 // allocated offsets (ADR-014: allocation time may precede durability)
	visibleHWM int64 // durable records only: what Fetch and HighWatermark expose (invariant 2)
}

// pendingFrame is one staged-but-unflushed record. Offsets are assigned at
// flush time, in staging order.
type pendingFrame struct {
	ts    int64
	key   []byte
	value []byte
	wait  chan flushResult
}

type flushResult struct {
	off int64
	err error
}

type segment struct {
	baseOffset int64
	path       string
	file       *os.File
	writePos   int64
	maxTs      int64
	index      []indexEntry

	// ADR-007 (plan 002, phase A): rolling digest state for content-addressed
	// block descriptors. maintained incrementally on every frame written or
	// scanned.
	createdAt   time.Time
	digest      hash.Hash // SHA-256 over raw frame bytes
	crcXor      uint32
	firstOffset int64 // -1 until first frame
	lastOffset  int64 // -1 until first frame
	count       int64
	sealed      bool
	blockCache  *BlockDescriptor
}

type indexEntry struct {
	offset int64
	pos    int64
}

// segmentFileName renders the segment file name for a base offset.
func segmentFileName(base int64) string { return fmt.Sprintf("%020d%s", base, segmentSuffix) }

func parseSegmentFileName(name string) (int64, bool) {
	base, ok := strings.CutSuffix(name, segmentSuffix)
	if !ok {
		return 0, false
	}
	off, err := strconv.ParseInt(base, 10, 64)
	if err != nil || off < 0 || base != fmt.Sprintf("%020d", off) {
		return 0, false
	}
	return off, true
}

// ErrOffsetOutOfRange is returned by Fetch when the requested offset is
// below the retention horizon (the oldest records were deleted by retention).
var ErrOffsetOutOfRange = errors.New("firn/log: offset below retention horizon")

// namedSegment pairs a parsed segment base offset with its file name.
type namedSegment struct {
	base int64
	name string
}

// Open opens (creating if necessary) the log in dir and performs recovery.
func Open(dir string, opts ...Option) (*FileLog, error) {
	l := &FileLog{dir: dir, maxSegmentBytes: defaultMaxSegmentBytes, syncOnAppend: true, maxBatchBytes: 1 << 20}
	for _, o := range opts {
		o(l)
	}

	if err := resolveCompactDirs(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("firn/log: mkdir %s: %w", dir, err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("firn/log: readdir %s: %w", dir, err)
	}
	var segs []namedSegment
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		base, ok := parseSegmentFileName(e.Name())
		if !ok {
			continue // ignore foreign files
		}
		segs = append(segs, namedSegment{base: base, name: e.Name()})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].base < segs[j].base })

	if len(segs) == 0 {
		seg, err := l.createSegment(0)
		if err != nil {
			return nil, err
		}
		l.segments = []*segment{seg}
		return l, nil
	}

	// Recover, segment by segment, in order.
	return l.recoverSegments(segs)
}

// recoverSegments scans all segments in order, validates frames, truncates a
// torn tail in the final segment, and rebuilds the in-memory index.
// The first segment's base offset is the retention horizon; earlier records
// may be gone, so offsets are expected to start at the horizon.
// recoverSegments is called during Open, before the handle is shared.
func (l *FileLog) recoverSegments(segs []namedSegment) (*FileLog, error) {
	hwm := segs[0].base
	for i, s := range segs {
		last := i == len(segs)-1
		seg, err := l.openSegment(s.base, filepath.Join(l.dir, s.name))
		if err != nil {
			return nil, err
		}
		if seg.baseOffset != hwm {
			_ = seg.file.Close()
			return nil, fmt.Errorf("firn/log: segment %s has base offset %d, want %d", seg.path, seg.baseOffset, hwm)
		}

		pos := int64(0)
		recCount := 0
		// Scan frames until EOF or the first torn/corrupt frame.
		for {
			rec, raw, ok, err := readFrameAt(seg.file, pos)
			if err != nil {
				_ = seg.file.Close()
				return nil, fmt.Errorf("firn/log: scan %s: %w", seg.path, err)
			}
			if !ok {
				break // clean or torn tail: stop here
			}
			if rec.Offset != hwm {
				_ = seg.file.Close()
				return nil, fmt.Errorf("firn/log: offset gap in %s at %d: frame has offset %d, want %d", seg.path, pos, rec.Offset, hwm)
			}
			if rec.Ts > seg.maxTs {
				seg.maxTs = rec.Ts
			}
			feedSegmentDigest(seg, raw, rec)

			if recCount%sparseIndexInterval == 0 {
				seg.index = append(seg.index, indexEntry{offset: rec.Offset, pos: pos})
			}
			pos += int64(len(raw))
			hwm++
			recCount++
		}

		st, err := seg.file.Stat()
		if err != nil {
			_ = seg.file.Close()
			return nil, fmt.Errorf("firn/log: stat %s: %w", seg.path, err)
		}
		switch {
		case st.Size() > pos:
			if !last {
				_ = seg.file.Close()
				return nil, fmt.Errorf("firn/log: corrupt segment %s at %d (of %d bytes): refusing to truncate a non-final segment", seg.path, pos, st.Size())
			}
			// Torn tail in the final segment: truncate.
			if err := seg.file.Truncate(pos); err != nil {
				_ = seg.file.Close()
				return nil, fmt.Errorf("firn/log: truncate torn tail %s: %w", seg.path, err)
			}
			if err := seg.file.Sync(); err != nil {
				_ = seg.file.Close()
				return nil, fmt.Errorf("firn/log: sync %s after truncation: %w", seg.path, err)
			}
		}
		seg.writePos = pos
		if !last {
			// All non-final segments are sealed blocks.
			seg.sealed = true
		}
		l.segments = append(l.segments, seg)
	}
	l.hwm = hwm
	l.visibleHWM = hwm
	return l, nil
}

// SealIfOlderThan rolls the active segment (producing a sealed block) only
// if it has existed for at least d and holds records. Returns true if a
// seal happened. Any staged flush-window batch is flushed first (ADR-014:
// a sealed block must be durable).
func (l *FileLog) SealIfOlderThan(d time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.flushLocked()
	active := l.segments[len(l.segments)-1]
	if active.writePos == 0 || time.Since(active.createdAt) < d {
		return false, nil
	}
	if err := l.syncSegmentLocked(active); err != nil {
		return false, fmt.Errorf("firn/log: seal sync: %w", err)
	}
	active.sealed = true
	seg, err := l.createSegment(l.hwm)
	if err != nil {
		return false, fmt.Errorf("firn/log: seal create segment: %w", err)
	}
	l.segments = append(l.segments, seg)
	return true, nil
}

// Append stores value and returns the assigned offset. The record timestamp
// is set by the broker time source (time.Now).
func (l *FileLog) Append(key, value []byte) int64 {
	return l.appendWithTimestamp(key, value, time.Now().UnixMilli())
}

// appendWithTimestamp is Append with an explicit record timestamp.
// Dispatches to the direct path (fsync-per-append, v0.4.0 semantics) or the
// ADR-014 flush-window path, depending on -flush-window.
func (l *FileLog) appendWithTimestamp(key, value []byte, ts int64) int64 {
	if l.flushWindow > 0 {
		return l.appendWindowed(key, value, ts)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	off := l.appendLockedFrame(key, value, ts)
	if l.syncOnAppend {
		active := l.segments[len(l.segments)-1]
		if err := l.syncSegmentLocked(active); err != nil {
			panic(fmt.Sprintf("firn/log: sync append: %v", err))
		}
	}
	l.visibleHWM = l.hwm
	return off
}

// appendLockedFrame writes one frame to the active segment (rolling if
// needed), assigns the next offset, and updates bookkeeping. It does not
// fsync; callers decide when to sync. Caller must hold l.mu.
func (l *FileLog) appendLockedFrame(key, value []byte, ts int64) int64 {
	off := l.hwm
	frame := encodeFrame(off, ts, key, value)

	active := l.segments[len(l.segments)-1]
	frameLen := int64(len(frame))
	sealDue := l.sealAge > 0 && time.Since(active.createdAt) >= l.sealAge
	if active.writePos > 0 && (active.writePos+frameLen > l.maxSegmentBytes || sealDue) {
		if err := l.syncSegmentLocked(active); err != nil {
			panic(fmt.Sprintf("firn/log: sync on segment roll: %v", err))
		}
		active.sealed = true // no more appends to this block; descriptor is stable
		seg, err := l.createSegment(off)
		if err != nil {
			panic(fmt.Sprintf("firn/log: create segment: %v", err))
		}
		l.segments = append(l.segments, seg)
		active = seg
	}

	l.writeFrameLocked(active, frame, off, ts, key, value)
	l.hwm++
	return off
}

// writeFrameLocked writes an already-encoded frame and updates digest,
// index, and writePos. Caller must hold l.mu.
func (l *FileLog) writeFrameLocked(active *segment, frame []byte, off, ts int64, key, value []byte) {
	n, err := active.file.WriteAt(frame, active.writePos)
	if err != nil || n != len(frame) {
		panic(fmt.Sprintf("firn/log: write %s: %v (%d/%d bytes)", active.path, err, n, len(frame)))
	}
	feedSegmentDigest(active, frame, frameRecord{Offset: off, Ts: ts, Key: key, Value: value})
	if ts > active.maxTs {
		active.maxTs = ts
	}
	if off%sparseIndexInterval == 0 {
		active.index = append(active.index, indexEntry{offset: off, pos: active.writePos})
	}
	active.writePos += int64(n)
	if l.flushWriteHook != nil && l.flushWindow > 0 {
		l.flushWriteHook(off)
	}
}

// Fetch returns up to max records starting at offset. It returns an error if
// offset is negative or >= the high watermark — the same contract as MemLog.
// Records at offset >= the high watermark are never returned (invariant 2).
func (l *FileLog) Fetch(offset int64, max int) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fetch(offset, max)
}

// fetch is Fetch without the mutex. Caller must hold l.mu.
// The visibility clamp is visibleHWM (durable records only): unflushed
// window frames, though staged in memory, are never returned.
func (l *FileLog) fetch(offset int64, max int) ([]Record, error) {
	if offset < 0 {
		return nil, errors.New("firn/log: negative offset")
	}
	if offset < l.retentionHorizonLocked() {
		return nil, ErrOffsetOutOfRange
	}
	if offset >= l.visibleHWM {
		return nil, errors.New("firn/log: offset beyond end of log")
	}
	if max < 1 {
		return nil, errors.New("firn/log: max must be >= 1")
	}

	segIdx := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > offset
	}) - 1

	out := make([]Record, 0, max)
	for i := segIdx; i < len(l.segments) && len(out) < max; i++ {
		seg := l.segments[i]
		pos := int64(0)
		if i == segIdx {
			if j := sort.Search(len(seg.index), func(k int) bool { return seg.index[k].offset > offset }) - 1; j >= 0 {
				pos = seg.index[j].pos
			}
		}
		for pos < seg.writePos && len(out) < max {
			rec, raw, ok, err := readFrameAt(seg.file, pos)
			if err != nil {
				return nil, fmt.Errorf("firn/log: read %s: %w", seg.path, err)
			}
			if !ok {
				return nil, fmt.Errorf("firn/log: corrupt frame in %s at %d during fetch", seg.path, pos)
			}
			if rec.Offset >= l.visibleHWM {
				break
			}
			if rec.Offset >= offset {
				out = append(out, copyRecord(rec))
			}
			pos += int64(len(raw))
		}
	}
	return out, nil
}

// fetchLocked is Fetch for the window-flush path: caller must hold l.mu.
func (l *FileLog) fetchLocked(offset int64, max int) ([]Record, error) {
	return l.fetch(offset, max)
}

// HighWatermark returns the offset of the next record to be written.
// With the ADR-014 flush window, unflushed staged frames never reach the
// segment, so the watermark already reflects durable records only.
func (l *FileLog) HighWatermark() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.visibleHWM
}

// Flush fsyncs the active segment. I/O errors from previous appends panic
// at the call site; Flush surfaces fsync failure as an error.
func (l *FileLog) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	active := l.segments[len(l.segments)-1]
	fsyncStart := time.Now()
	if err := active.file.Sync(); err != nil {
		return fmt.Errorf("firn/log: flush: %w", err)
	}
	observeFsync(fsyncStart)
	return nil
}

// Close fsyncs and closes all segments. Any staged flush-window batch is
// flushed first (ADR-014); if the rollback after a flush failure poisoned
// the log, Close surfaces it in the returned error (appends have already
// failed for the waiting producers).
func (l *FileLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.flushLocked()
	var firstErr error
	if l.flushPoisoned {
		firstErr = fmt.Errorf("firn/log: flush window poisoned during close (rollback failure)")
	}
	for _, seg := range l.segments {
		if err := l.syncSegmentLocked(seg); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("firn/log: close sync %s: %w", seg.path, err)
		}
		if err := seg.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("firn/log: close %s: %w", seg.path, err)
		}
	}
	return firstErr
}

// RetentionHorizon returns the oldest offset still stored. Fetching offsets
// below it returns ErrOffsetOutOfRange. Equals 0 when retention has not run.
func (l *FileLog) RetentionHorizon() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.retentionHorizonLocked()
}

// retentionHorizonLocked is RetentionHorizon without the lock.
// Callers must hold l.mu.
func (l *FileLog) retentionHorizonLocked() int64 {
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].baseOffset
}

// RetainOlderThan deletes whole segments whose newest record is older than
// cutoff. The active segment is never deleted. It returns the number of
// segments deleted. (Retention deletes whole segments only — ADR-003.)
func (l *FileLog) RetainOlderThan(cutoff time.Time) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cutoffMs := cutoff.UnixMilli()
	n := 0
	// Segments are time-ordered (ts is nondecreasing across appends), so a
	// deletable prefix of non-active segments is all that can qualify.
	for len(l.segments) > 1 {
		s := l.segments[0]
		if s.maxTs == 0 || s.maxTs >= cutoffMs {
			break
		}
		if err := l.deleteSegments(1); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// RetainOverBytes deletes the oldest whole segments until the total size of
// the remaining segments fits maxBytes. The active segment is never deleted,
// even if it alone exceeds the budget. maxBytes <= 0 disables the policy.
func (l *FileLog) RetainOverBytes(maxBytes int64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if maxBytes <= 0 {
		return 0, nil
	}
	var total int64
	sizes := make([]int64, len(l.segments))
	for i, s := range l.segments {
		sz, err := segmentSize(s)
		if err != nil {
			return 0, err
		}
		sizes[i] = sz
		total += sz
	}
	n := 0
	for i := 0; total > maxBytes && i < len(l.segments)-1; i++ {
		total -= sizes[i]
		n++
	}
	if n == 0 {
		return 0, nil
	}
	if err := l.deleteSegments(n); err != nil {
		return 0, err
	}
	return n, nil
}

// deleteSegments removes the n oldest segments (never the active one) and
// fsyncs the directory so the deletions survive a crash.
// Callers must hold l.mu.
func (l *FileLog) deleteSegments(n int) error {
	if n >= len(l.segments) {
		return fmt.Errorf("firn/log: refusing to delete %d of %d segments (active segment is never deleted)", n, len(l.segments))
	}
	for i := 0; i < n; i++ {
		s := l.segments[i]
		if err := s.file.Close(); err != nil {
			return fmt.Errorf("firn/log: close segment before delete %s: %w", s.path, err)
		}
		if err := os.Remove(s.path); err != nil {
			return fmt.Errorf("firn/log: delete segment %s: %w", s.path, err)
		}
	}
	l.segments = l.segments[n:]
	d, err := os.Open(l.dir)
	if err != nil {
		return fmt.Errorf("firn/log: open dir for fsync %s: %w", l.dir, err)
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return fmt.Errorf("firn/log: fsync dir %s: %w", l.dir, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("firn/log: close dir %s: %w", l.dir, closeErr)
	}
	return nil
}

// segmentSize returns the on-disk size of s.
func segmentSize(s *segment) (int64, error) {
	st, err := s.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("firn/log: stat %s: %w", s.path, err)
	}
	return st.Size(), nil
}

// createSegment opens a new active segment. Caller must hold l.mu, or be
// running inside Open (pre-publication).
func (l *FileLog) createSegment(base int64) (*segment, error) {
	path := filepath.Join(l.dir, segmentFileName(base))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("firn/log: create segment %s: %w", path, err)
	}
	return newSegment(base, path, f, time.Now()), nil
}

// openSegment opens an existing segment. Caller must hold l.mu, or be
// running inside Open (pre-publication).
func (l *FileLog) openSegment(base int64, path string) (*segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("firn/log: open segment %s: %w", path, err)
	}
	return newSegment(base, path, f, time.Now()), nil
}

func newSegment(base int64, path string, f *os.File, createdAt time.Time) *segment {
	return &segment{
		baseOffset:  base,
		path:        path,
		file:        f,
		createdAt:   createdAt,
		digest:      sha256.New(),
		firstOffset: -1,
		lastOffset:  -1,
	}
}

type frameRecord struct {
	Offset int64
	Ts     int64
	Key    []byte
	Value  []byte
}

func encodeFrame(offset, ts int64, key, value []byte) []byte {
	keyLen, valueLen := len(key), len(value)
	bodyLen := 4 + 8 + 8 + 4 + 4 + keyLen + valueLen // crc + offset + ts + keyLen + valueLen + payloads
	buf := make([]byte, 4+bodyLen)

	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))
	body := buf[8:] // crc goes in [4:8] after it's computed
	binary.BigEndian.PutUint64(body[0:8], uint64(offset))
	binary.BigEndian.PutUint64(body[8:16], uint64(ts))
	binary.BigEndian.PutUint32(body[16:20], uint32(keyLen))
	binary.BigEndian.PutUint32(body[20:24], uint32(valueLen))
	copy(body[24:], key)
	copy(body[24+keyLen:], value)

	crc := crc32.ChecksumIEEE(buf[8:])
	binary.BigEndian.PutUint32(buf[4:8], crc)
	return buf
}

// readFrameAt reads and CRC-validates the frame at pos. ok is false at a
// clean EOF or a torn/corrupt tail (caller decides whether to truncate).
// raw is the complete frame including the length prefix; callers that only
// need pos arithmetic can use len(raw) instead of tracking frameLen.
func readFrameAt(f *os.File, pos int64) (rec frameRecord, raw []byte, ok bool, err error) {
	var lenBuf [4]byte
	if _, err := f.ReadAt(lenBuf[:], pos); err != nil {
		if errors.Is(err, io.EOF) {
			return frameRecord{}, nil, false, nil // no full length prefix
		}
		return frameRecord{}, nil, false, err
	}
	bodyLen := binary.BigEndian.Uint32(lenBuf[:])
	if bodyLen < 4+8+8+4+4 || bodyLen > 1<<28 { // [min, 256 MiB]: implausible length, torn tail
		return frameRecord{}, nil, false, nil
	}
	buf := make([]byte, bodyLen)
	n, err := f.ReadAt(buf, pos+4)
	if err != nil || n != len(buf) {
		return frameRecord{}, nil, false, nil // torn tail
	}
	body := buf[4:]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(buf[0:4]) {
		return frameRecord{}, nil, false, nil // CRC mismatch: corrupt frame
	}
	offset := int64(binary.BigEndian.Uint64(body[0:8]))
	ts := int64(binary.BigEndian.Uint64(body[8:16]))
	keyLen := int64(binary.BigEndian.Uint32(body[16:20]))
	valueLen := int64(binary.BigEndian.Uint32(body[20:24]))
	if int64(len(body)) != 24+keyLen+valueLen {
		return frameRecord{}, nil, false, nil // length fields inconsistent
	}
	rec = frameRecord{
		Offset: offset,
		Ts:     ts,
		Key:    append([]byte(nil), body[24:24+keyLen]...),
		Value:  append([]byte(nil), body[24+keyLen:]...),
	}
	raw = append(append([]byte(nil), lenBuf[:]...), buf[:]...)
	return rec, raw, true, nil
}

func copyRecord(r frameRecord) Record {
	return Record{Offset: r.Offset, Key: r.Key, Value: r.Value}
}

// ---------------- ADR-014 flush window ----------------

// pendingFramesLocked returns the staged frames. Caller must hold l.mu.
// Test-only: the result is read without locks by tests holding l.mu.
func (l *FileLog) pendingFramesLocked() []pendingFrame { return l.pending }

// syncSegmentLocked fsyncs seg through one choke point (so tests can fault
// it). Caller must hold l.mu.
func (l *FileLog) syncSegmentLocked(seg *segment) error {
	fsyncStart := time.Now()
	err := seg.file.Sync()
	observeFsync(fsyncStart)
	return err
}

// armFlushTimerLocked (re)arms the window timer. Called when the first
// frame of a window is staged; byte-full windows flush inline instead.
func (l *FileLog) armFlushTimerLocked() {
	l.stopFlushTimerLocked()
	l.flushTimer = time.AfterFunc(l.flushWindow, l.flush)
}

func (l *FileLog) stopFlushTimerLocked() {
	if l.flushTimer != nil {
		l.flushTimer.Stop()
		l.flushTimer = nil
	}
}

// appendWindowed stages the record and blocks until the window holding it
// is durable. On window failure every caller in the window wakes with the
// same error (surfaced as a panic, consistent with the rest of FileLog).
func (l *FileLog) appendWindowed(key, value []byte, ts int64) int64 {
	l.mu.Lock()
	if l.flushPoisoned {
		l.mu.Unlock()
		panic("firn/log: flush window poisoned after rollback failure; restart required")
	}
	fr := pendingFrame{ts: ts, key: key, value: append([]byte(nil), value...), wait: make(chan flushResult, 1)}
	l.pending = append(l.pending, fr)
	l.stagingBytes += int64(len(key) + len(value) + 64)
	full := l.stagingBytes >= l.maxBatchBytes
	if len(l.pending) == 1 {
		l.armFlushTimerLocked()
	}
	l.mu.Unlock()
	if full {
		l.flush() // byte-full: flush inline, no timer wait
	}
	res := <-fr.wait
	if res.err != nil {
		panic(fmt.Sprintf("firn/log: flush window failed: %v", res.err))
	}
	return res.off
}

// flush writes the staged batch to the segment in order (rolling segments
// per the usual rules), fsyncs, then publishes the records: hwm/visibleHWM
// advance and every blocked producer wakes with its offset.
//
// On any failure the flush rolls back deterministically: every segment the
// batch wrote into (including any segment this flush created) is truncated
// back to its pre-flush position (created segments are deleted), segment
// digests/indexes/maxTs are rebuilt from disk, hwm is reset to the
// pre-flush value, and every producer in the window receives the error.
// If the rollback itself fails, the log is poisoned fail-closed.
func (l *FileLog) flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.flushLocked()
}

// flushLocked is flush without the mutex; caller must hold l.mu.
func (l *FileLog) flushLocked() {
	l.stopFlushTimerLocked()
	if len(l.pending) == 0 || l.flushPoisoned {
		return
	}
	staged := l.pending
	flushStartHWM := l.hwm
	n := int64(len(staged))
	l.hwm += n // reserve offsets for the window; rolled back on failure

	type touchedSeg struct {
		seg        *segment
		origWrite  int64
		origSealed bool
		created    bool
	}
	touched := []touchedSeg{}
	markTouched := func(seg *segment, created bool) {
		for i := range touched {
			if touched[i].seg == seg {
				return
			}
		}
		touched = append(touched, touchedSeg{seg: seg, origWrite: seg.writePos, origSealed: seg.sealed, created: created})
	}

	fail := func(cause error) {
		backErr := cause
		for i := len(touched) - 1; i >= 0; i-- {
			t := touched[i]
			if t.created {
				_ = t.seg.file.Close()
				if err := os.Remove(t.seg.path); err != nil {
					backErr = fmt.Errorf("%w (rollback remove %s: %v)", backErr, t.seg.path, err)
					l.flushPoisoned = true
					continue
				}
				if m := len(l.segments); m > 0 && l.segments[m-1] == t.seg {
					l.segments = l.segments[:m-1]
				}
				continue
			}
			if err := t.seg.file.Truncate(t.origWrite); err != nil {
				backErr = fmt.Errorf("%w (rollback truncate %s: %v)", backErr, t.seg.path, err)
				l.flushPoisoned = true
				continue
			}
			t.seg.writePos = t.origWrite
			t.seg.sealed = t.origSealed
			if err := t.seg.file.Sync(); err != nil {
				backErr = fmt.Errorf("%w (rollback sync %s: %v)", backErr, t.seg.path, err)
				l.flushPoisoned = true
			}
		}
		for _, t := range touched {
			if t.created {
				continue
			}
			if err := l.rebuildSegmentStateLocked(t.seg, flushStartHWM); err != nil {
				backErr = fmt.Errorf("%w (rollback rebuild %s: %v)", backErr, t.seg.path, err)
				l.flushPoisoned = true
			}
		}
		l.hwm = flushStartHWM
		l.pending = nil
		l.stagingBytes = 0
		for _, fr := range staged {
			fr.wait <- flushResult{err: backErr}
		}
	}

	for i, fr := range staged {
		off := flushStartHWM + int64(i)
		frame := encodeFrame(off, fr.ts, fr.key, fr.value)
		frameLen := int64(len(frame))

		active := l.segments[len(l.segments)-1]
		sealDue := l.sealAge > 0 && time.Since(active.createdAt) >= l.sealAge
		if active.writePos > 0 && (active.writePos+frameLen > l.maxSegmentBytes || sealDue) {
			// Sealing here is part of the batch: the old active becomes a
			// durable block before it is sealed.
			markTouched(active, false)
			if err := l.syncSegmentLocked(active); err != nil {
				fail(fmt.Errorf("firn/log: sync before segment seal: %w", err))
				return
			}
			active.sealed = true
			seg, err := l.createSegment(off)
			if err != nil {
				fail(fmt.Errorf("firn/log: create segment: %w", err))
				return
			}
			markTouched(seg, true)
			l.segments = append(l.segments, seg)
			active = seg
		}
		markTouched(active, false)

		l.writeFrameLocked(active, frame, off, fr.ts, fr.key, fr.value)
	}

	active := l.segments[len(l.segments)-1]
	// The batch sync: THE fsync that makes the whole window durable.
	// injectedSyncErr faults only here (roll/segment syncs stay real).
	fsyncStart := time.Now()
	var syncErr error
	if l.injectedSyncErr != nil {
		syncErr = l.injectedSyncErr
	} else {
		syncErr = active.file.Sync()
	}
	observeFsync(fsyncStart)
	if syncErr != nil {
		fail(fmt.Errorf("firn/log: fsync window: %w", syncErr))
		return
	}

	l.visibleHWM = l.hwm
	l.pending = nil
	l.stagingBytes = 0
	for i, fr := range staged {
		fr.wait <- flushResult{off: flushStartHWM + int64(i)}
	}
}

// rebuildSegmentStateLocked re-derives digest, maxTs, and the sparse index
// for seg by re-scanning its on-disk frames (used during flush rollback,
// where staged frames were written to seg and then erased by truncation).
// boundary is the first offset that was rolled back; every on-disk frame
// must fall below it. Caller must hold l.mu; seg.writePos must already
// reflect the rollback.
func (l *FileLog) rebuildSegmentStateLocked(seg *segment, boundary int64) error {
	seg.digest = sha256.New()
	seg.crcXor = 0
	seg.firstOffset, seg.lastOffset = -1, -1
	seg.count = 0
	seg.maxTs = 0
	seg.index = seg.index[:0]
	pos := int64(0)
	for pos < seg.writePos {
		rec, raw, ok, err := readFrameAt(seg.file, pos)
		if err != nil {
			return fmt.Errorf("rescan %s at %d: %w", seg.path, pos, err)
		}
		if !ok {
			return fmt.Errorf("corrupt frame during rollback rescan at %s:%d", seg.path, pos)
		}
		if rec.Offset >= boundary {
			return fmt.Errorf("rollback rescan %s: frame offset %d >= rollback boundary %d — writePos inconsistent", seg.path, rec.Offset, boundary)
		}
		feedSegmentDigest(seg, raw, rec)
		if rec.Ts > seg.maxTs {
			seg.maxTs = rec.Ts
		}
		if rec.Offset%sparseIndexInterval == 0 {
			seg.index = append(seg.index, indexEntry{offset: rec.Offset, pos: pos})
		}
		pos += int64(len(raw))
	}
	return nil
}

// AppendBatch appends many records in order and fsyncs once at the end.
// Rollback on failure is identical in contract to the flush window:
// every segment the batch wrote into is truncated to its pre-batch
// position (segments created by the batch are deleted; digests/indexes
// rebuilt from disk), so no partial batch can become visible.
func (l *FileLog) AppendBatch(recs []BatchRecord) ([]int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(recs) == 0 {
		return nil, nil
	}
	hwmStart := l.hwm

	type touchedSeg struct {
		seg          *segment
		origWritePos int64
		origSealed   bool
		created      bool
	}
	touched := []touchedSeg{}
	markTouched := func(seg *segment, created bool) {
		for i := range touched {
			if touched[i].seg == seg {
				return
			}
		}
		touched = append(touched, touchedSeg{seg: seg, origWritePos: seg.writePos, origSealed: seg.sealed, created: created})
	}

	snapshotRoll := func(active *segment, off int64, sealDue bool) (*segment, error) {
		markTouched(active, false)
		if err := l.syncSegmentLocked(active); err != nil {
			return nil, fmt.Errorf("firn/log: sync before segment seal: %w", err)
		}
		active.sealed = true
		seg, err := l.createSegment(off)
		if err != nil {
			return nil, fmt.Errorf("firn/log: create segment: %w", err)
		}
		markTouched(seg, true)
		l.segments = append(l.segments, seg)
		return seg, nil
	}

	writeErr := error(nil)
	type placement struct {
		frame []byte
		off   int64
		ts    int64
		key   []byte
		value []byte
	}
	placed := make([]placement, 0, len(recs))
	nowTs := time.Now().UnixMilli()
	for i, r := range recs {
		off := hwmStart + int64(i)
		frame := encodeFrame(off, nowTs, r.Key, r.Value)
		p := placement{frame: frame, off: off, ts: nowTs, key: r.Key, value: r.Value}
		// Decide the segment for this frame before writing: rolls happen
		// exactly as in the flush path so segment accounting stays local.
		active := l.segments[len(l.segments)-1]
		sealDue := l.sealAge > 0 && time.Since(active.createdAt) >= l.sealAge
		if active.writePos > 0 && (active.writePos+int64(len(frame)) > l.maxSegmentBytes || sealDue) {
			var err error
			if active, err = snapshotRoll(active, off, sealDue); err != nil {
				writeErr = err
				break
			}
			_ = sealDue
		}
		markTouched(active, false)
		l.writeFrameLocked(active, p.frame, p.off, p.ts, p.key, p.value)
		l.hwm++
		placed = append(placed, p)
	}
	if writeErr == nil {
		active := l.segments[len(l.segments)-1]
		if err := l.syncSegmentLocked(active); err != nil {
			writeErr = fmt.Errorf("firn/log: fsync batch: %w", err)
		}
	}
	if writeErr != nil {
		backErr := writeErr
		for i := len(touched) - 1; i >= 0; i-- {
			t := touched[i]
			if t.created {
				_ = t.seg.file.Close()
				_ = os.Remove(t.seg.path)
				if n := len(l.segments); n > 0 && l.segments[n-1] == t.seg {
					l.segments = l.segments[:n-1]
				}
				continue
			}
			if err := t.seg.file.Truncate(t.origWritePos); err != nil {
				backErr = fmt.Errorf("%w (rollback truncate %s: %v)", backErr, t.seg.path, err)
				continue
			}
			t.seg.writePos = t.origWritePos
			t.seg.sealed = t.origSealed
			_ = t.seg.file.Sync()
			_ = l.rebuildSegmentStateLocked(t.seg, hwmStart)
		}
		l.hwm = hwmStart
		return nil, backErr
	}
	l.visibleHWM = l.hwm
	offs := make([]int64, 0, len(placed))
	for _, p := range placed {
		offs = append(offs, p.off)
	}
	return offs, nil
}

// ---------------- end of AppendBatch ----------------
