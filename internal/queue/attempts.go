package queue

// Attempt persistence for queue deliveries.
//
// Attempts are tracked in the internal __queue_attempts topic
// (<dir>/__queue_attempts-0, or an in-memory MemLog), compacted the same
// way as __consumer_offsets (docs/plans/001-mvp-single-node.md, slice 5):
//
//	key   = queue\x00partition\x00offset     (offset in ASCII)
//	value = u32 BE delivery-attempt count
//
// The folded state (max attempts per key) is cached in memory; the disk log
// is compacted every 1000 increments and on Close.

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/rahulkj/firn/internal/fence"
	"github.com/rahulkj/firn/internal/log"
)

type attemptLog interface {
	Append(key, value []byte) int64
	Fetch(offset int64, max int) ([]log.Record, error)
	HighWatermark() int64
}

type AttemptStore struct {
	inner               attemptLog
	dir                 string
	sharedFl            bool
	cache               map[string]uint32
	foldedHWM           int64
	commitsSinceCompact int
	gate                *fence.GateBox
}

// SetGate binds the store to an internal-topic write fence (see
// internal/fence). Fenced (cluster-mode) stores fail closed on writes and
// refuse compaction because renumbering records would diverge the
// replicated log.
func (s *AttemptStore) SetGate(g *fence.GateBox) { s.gate = g }

// NewAttemptStore opens (or creates) the store rooted at dir ("" = in-memory). An
// already-opened FileLog at __queue_attempts-0 is used when provided.
func NewAttemptStore(dir string, fl *log.FileLog) (*AttemptStore, error) {
	s := &AttemptStore{}
	if dir == "" {
		s.inner = &log.MemLog{}
	} else {
		shared := fl != nil
		if fl == nil {
			var err error
			fl, err = log.Open(filepath.Join(dir, "__queue_attempts-0"))
			if err != nil {
				return nil, fmt.Errorf("firn/queue: open attempts store: %w", err)
			}
		}
		s.inner = fl
		s.dir = dir
		s.sharedFl = shared
	}
	s.cache = map[string]uint32{}
	hw := s.inner.HighWatermark()
	if hw > 0 {
		if err := s.fold(); err != nil {
			return nil, fmt.Errorf("firn/queue: read attempts store: %w", err)
		}
	}
	return s, nil
}

// fold re-reads the whole log and replaces the cache. Caller-owned; HWM
// tracking is the caller's concern.
func (s *AttemptStore) fold() error {
	hw := s.inner.HighWatermark()
	out := map[string]uint32{}
	if hw > 0 {
		recs, err := s.inner.Fetch(0, int(hw))
		if err != nil {
			return err
		}
		for _, r := range recs {
			if len(r.Value) != 4 {
				return fmt.Errorf("firn/queue: corrupt attempts record (%d bytes)", len(r.Value))
			}
			out[string(r.Key)] = binary.BigEndian.Uint32(r.Value)
		}
	}
	s.cache = out
	s.foldedHWM = hw
	return nil
}

func (s *AttemptStore) refreshIfMoved() {
	if hw := s.inner.HighWatermark(); hw != s.foldedHWM {
		_ = s.fold() // corrupt tail: keep serving the last known-good cache
	}
}

func attemptKey(queue string, partition int32, offset int64) []byte {
	return []byte(fmt.Sprintf("%s\x00%d\x00%d", queue, partition, offset))
}

// Attempts returns the folded attempt count for (queue, partition, offset).
// The fold is re-read only when the high watermark moved (O(1) otherwise),
// so replicated commits are visible on every node without a refresh hook.
func (s *AttemptStore) Attempts(queue string, partition int32, offset int64) uint32 {
	s.refreshIfMoved()
	return s.cache[string(attemptKey(queue, partition, offset))]
}

// Put records attempts for the message and opportunistically compacts.
// Fenced (cluster-mode) stores fail closed and never compact — the
// replicated log must stay byte-identical across the cluster.
func (s *AttemptStore) Put(queue string, partition int32, offset int64, attempts uint32) error {
	if err := s.gate.Allow("__queue_attempts"); err != nil {
		return err
	}
	key := attemptKey(queue, partition, offset)
	s.inner.Append(key, binary.BigEndian.AppendUint32(nil, attempts))
	s.cache[string(key)] = attempts
	s.foldedHWM = s.inner.HighWatermark()
	s.commitsSinceCompact++
	if s.gate == nil || s.gate.Allowed() {
		if s.dir != "" && s.commitsSinceCompact >= compactEveryCommits {
			if fl, ok := s.inner.(*log.FileLog); ok {
				if err := fl.Compact(); err != nil {
					return fmt.Errorf("firn/queue: compact attempts store: %w", err)
				}
				s.commitsSinceCompact = 0
				s.foldedHWM = s.inner.HighWatermark()
			}
		}
	}
	return nil
}

// Compact refuses when the store is fenced (replicated internal log).
func (s *AttemptStore) Compact() error {
	if s.gate != nil && !s.gate.Allowed() {
		return fmt.Errorf("firn/queue: compact refused for replicated internal log __queue_attempts")
	}
	if fl, ok := s.inner.(*log.FileLog); ok {
		return fl.Compact()
	}
	return nil
}

// Close compacts (file-backed store) and closes the log.
func (s *AttemptStore) Close() error {
	fl, ok := s.inner.(*log.FileLog)
	if !ok {
		return nil
	}
	if s.sharedFl {
		// The broker owns the shared __queue_attempts handle; the store
		// must never close or compact it (replication depends on stable
		// lifecycles). Nothing to release.
		return nil
	}
	if s.gate != nil && !s.gate.Allowed() {
		return fl.Close() // own store; do not compact when the log is fenced
	}
	if err := fl.Compact(); err != nil {
		return fmt.Errorf("firn/queue: compact attempts store: %w", err)
	}
	return fl.Close()
}

const compactEveryCommits = 1000
