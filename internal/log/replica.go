package log

// Replication surface (ADR-007 phase C): raw frame I/O used by the
// replicator. ReadRange returns the exact on-disk bytes of frames in
// [firstOffset, lastOffset] so replicas can reconstruct identical segments.
// AppendReplicated appends with a caller-supplied timestamp; encoding is
// identical to the leader's, so replicated logs produce identical block IDs.

import (
	"errors"
	"fmt"
)

// ReadRange returns the raw bytes of frames with offsets in
// [firstOffset, lastOffset], in order (length prefix included). A range end
// beyond the high watermark is clamped to the end of the log — callers that
// want "everything from X to latest" use this without first querying HWM.
func (l *FileLog) ReadRange(firstOffset, lastOffset int64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if firstOffset < 0 || lastOffset < firstOffset {
		return nil, fmt.Errorf("firn/log: invalid range [%d, %d]", firstOffset, lastOffset)
	}
	if lastOffset >= l.hwm {
		lastOffset = l.hwm - 1
		if lastOffset < firstOffset {
			return nil, nil
		}
	}
	if firstOffset < l.retentionHorizonLocked() {
		return nil, ErrOffsetOutOfRange
	}
	// Locate starting segment.
	segIdx := -1
	for i, s := range l.segments {
		if firstOffset >= s.baseOffset {
			segIdx = i
		}
	}
	if segIdx == -1 {
		return nil, ErrOffsetOutOfRange
	}
	out := []byte{}
	for i := segIdx; i < len(l.segments); i++ {
		seg := l.segments[i]
		segPos := int64(0)
		for segPos < seg.writePos {
			rec, raw, ok, err := readFrameAt(seg.file, segPos)
			if err != nil {
				return nil, fmt.Errorf("firn/log: read %s: %w", seg.path, err)
			}
			if !ok {
				return nil, fmt.Errorf("firn/log: corrupt frame in %s at %d", seg.path, segPos)
			}
			if rec.Offset >= firstOffset {
				out = append(out, raw...)
			}
			segPos += int64(len(raw))
			if rec.Offset >= lastOffset {
				return out, nil
			}
		}
	}
	return out, nil
}

// AppendReplicated appends a record with the caller-supplied timestamp. The
// encoded frame is byte-identical to the leader's frame (same offset,
// timestamp, key, value), so replicated logs produce identical CRC/block
// IDs. The record is appended at the current high watermark, which is the
// assigned offset — out-of-order replication calls are a caller bug and are
// detectable by comparing the returned offset with the expected one.
func (l *FileLog) AppendReplicated(ts int64, key, value []byte) int64 {
	return l.appendWithTimestamp(key, value, ts)
}

// Timestamp returns the timestamp stored in the frame at offset. Replicators
// use it to preserve the original block identity.
func (l *FileLog) Timestamp(offset int64) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset < l.retentionHorizonLocked() {
		return 0, ErrOffsetOutOfRange
	}
	if offset >= l.hwm {
		return 0, errors.New("firn/log: offset beyond end of log")
	}
	segIdx := l.segmentIndexFor(offset)
	seg := l.segments[segIdx]
	pos := seg.indexPositionFor(offset)
	for pos < seg.writePos {
		rec, raw, ok, err := readFrameAt(seg.file, pos)
		if err != nil {
			return 0, fmt.Errorf("firn/log: read %s: %w", seg.path, err)
		}
		if !ok {
			return 0, fmt.Errorf("firn/log: corrupt frame in %s at %d", seg.path, pos)
		}
		if rec.Offset == offset {
			return rec.Ts, nil
		}
		pos += int64(len(raw))
	}
	return 0, errors.New("firn/log: offset not found")
}

// segmentIndexFor reports the segment index holding offset (or the last
// segment at or below it). Caller must hold l.mu.
func (l *FileLog) segmentIndexFor(offset int64) int {
	segIdx := -1
	for i, s := range l.segments {
		if offset >= s.baseOffset {
			segIdx = i
		}
	}
	return segIdx
}

func (s *segment) indexPositionFor(offset int64) int64 {
	pos := int64(0)
	j := 0
	for i, e := range s.index {
		if e.offset <= offset {
			pos = e.pos
			j = i
		} else {
			break
		}
	}
	_ = j
	return pos
}
