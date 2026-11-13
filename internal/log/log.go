// Package log implements firn's append-only log.
//
// Roadmap (see docs/plans/001-mvp-single-node.md):
//
//	slice 1: in-memory append/fetch (this file) — correctness first
//	slice 2: segmented file log + sparse index + crash recovery
//
// Invariants (docs/adr/003-storage.md):
//   - Offsets are assigned monotonically starting at 0.
//   - A fetch never returns records with offset >= the high watermark.
//   - After crash recovery, no torn tail record is visible to consumers.
package log

import "errors"

// Record is a single message in the log.
type Record struct {
	Offset int64
	Key    []byte
	Value  []byte
}

// MemLog is the in-memory slice-1 log. It is not durable and not
// concurrency-safe until the broker layer serializes access.
type MemLog struct {
	records []Record
}

// Append stores value and returns the assigned offset.
func (l *MemLog) Append(key, value []byte) int64 {
	off := int64(len(l.records))
	k := append([]byte(nil), key...)
	v := append([]byte(nil), value...)
	l.records = append(l.records, Record{Offset: off, Key: k, Value: v})
	return off
}

// Fetch returns up to max records starting at offset. It returns an error
// if offset is negative or beyond the end of the log.
func (l *MemLog) Fetch(offset int64, max int) ([]Record, error) {
	if offset < 0 {
		return nil, errors.New("firn/log: negative offset")
	}
	if offset >= int64(len(l.records)) {
		return nil, errors.New("firn/log: offset beyond end of log")
	}
	end := offset + int64(max)
	if end > int64(len(l.records)) {
		end = int64(len(l.records))
	}
	out := make([]Record, 0, end-offset)
	for _, r := range l.records[offset:end] {
		out = append(out, Record{Offset: r.Offset, Key: append([]byte(nil), r.Key...), Value: append([]byte(nil), r.Value...)})
	}
	return out, nil
}

// HighWatermark returns the offset of the next record to be written.
func (l *MemLog) HighWatermark() int64 { return int64(len(l.records)) }
