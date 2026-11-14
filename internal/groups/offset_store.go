package groups

// __consumer_offsets internal store (docs/plans/001-mvp-single-node.md,
// slice 4; ADR-004). Layout follows the partition-log naming convention:
// <dir>/__consumer_offsets-0, or an in-memory MemLog when dir == "".
//
// Records: key = group\x00topic\x00partition (NUL-separated; parts must not
// contain NUL — validated at the coordinator), value:
//
//	[offset i64][generation i64][leaderLen u32][leader bytes]
//
// The folded state (latest per key) is kept in memory and loaded at open,
// so reads never scan the log. Physical compaction (FileLog.Compact, called
// every 1000 commits and on clean shutdown) bounds the on-disk size.

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/rahulkj/firn/internal/fence"
	"github.com/rahulkj/firn/internal/log"
)

const compactEveryCommits = 1000

// partitionLog is the storage contract the offset store needs.
type partitionLog interface {
	Append(key, value []byte) int64
	Fetch(offset int64, max int) ([]log.Record, error)
	HighWatermark() int64
}

// CommittedOffset is one folded (latest) committed offset.
type CommittedOffset struct {
	Offset     int64
	Generation int64
	Leader     string
}

// OffsetStore is the internal log binding for committed group offsets.
type OffsetStore struct {
	inner partitionLog
	dir   string
	folds uint64
	// sharedFl is true when the store borrows the broker's shared
	// __consumer_offsets handle; ownership of that handle (and its
	// lifecycle) belongs to the broker, never to this store.
	sharedFl            bool
	cache               map[string]CommittedOffset
	foldedHWM           int64
	commitsSinceCompact int
	compactEvery        int
	gate                *fence.GateBox
}

// SetGate binds the store to an internal-topic write fence (see
// internal/fence). When a gate is set (cluster mode): writes fail closed
// on non-holders of the internal lease, and compaction is refused because
// renumbering records would diverge replicated logs.
func (s *OffsetStore) SetGate(g *fence.GateBox) { s.gate = g }

// NewOffsetStore opens (or creates) the store rooted at dir ("" = in-memory).
func NewOffsetStore(dir string, fl *log.FileLog) (*OffsetStore, error) {
	var s *OffsetStore
	if dir == "" {
		s = &OffsetStore{inner: &log.MemLog{}, compactEvery: compactEveryCommits}
	} else {
		if fl == nil {
			var err error
			fl, err = log.Open(filepath.Join(dir, "__consumer_offsets-0"))
			if err != nil {
				return nil, fmt.Errorf("firn/groups: open offsets store: %w", err)
			}
		}
		s = &OffsetStore{inner: fl, dir: dir, compactEvery: compactEveryCommits, sharedFl: fl != nil}
	}
	// Load folded state once.
	s.cache = map[string]CommittedOffset{}
	hw := s.inner.HighWatermark()
	if hw > 0 {
		recs, err := s.inner.Fetch(0, int(hw))
		if err != nil {
			return nil, fmt.Errorf("firn/groups: read offsets store: %w", err)
		}
		for _, r := range recs {
			v, err := decodeCommittedOffset(r.Value)
			if err != nil {
				return nil, err
			}
			s.cache[string(r.Key)] = v
		}
	}
	s.foldedHWM = hw
	return s, nil
}

// encodeCommittedOffset serializes o.
func encodeCommittedOffset(o CommittedOffset) []byte {
	leader := []byte(o.Leader)
	buf := make([]byte, 8+8+4+len(leader))
	binary.BigEndian.PutUint64(buf[0:8], uint64(o.Offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(o.Generation))
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(leader)))
	copy(buf[20:], leader)
	return buf
}

func decodeCommittedOffset(b []byte) (CommittedOffset, error) {
	if len(b) < 20 {
		return CommittedOffset{}, fmt.Errorf("firn/groups: committed offset record too short (%d bytes)", len(b))
	}
	leaderLen := int(binary.BigEndian.Uint32(b[16:20]))
	if len(b) != 20+leaderLen {
		return CommittedOffset{}, fmt.Errorf("firn/groups: committed offset record length mismatch: header says %d, total %d", 20+leaderLen, len(b))
	}
	return CommittedOffset{
		Offset:     int64(binary.BigEndian.Uint64(b[0:8])),
		Generation: int64(binary.BigEndian.Uint64(b[8:16])),
		Leader:     string(b[20:]),
	}, nil
}

// CommitKey builds the fold key for a (group, topic, partition).
func CommitKey(group, topic string, partition int32) []byte {
	return []byte(fmt.Sprintf("%s\x00%s\x00%d", group, topic, partition))
}

// Commit appends a committed offset, updates the folded cache, and
// opportunistically compacts the physical log.
func (s *OffsetStore) Commit(key []byte, o CommittedOffset) error {
	if err := s.gate.Allow("__consumer_offsets"); err != nil {
		return err
	}
	// Catch up on any cross-node writes before applying our own — without
	// this, committing against a stale cache could regress an offset a
	// replica already committed (fencing requires monotone commits).
	s.refreshIfMoved()
	s.inner.Append(key, encodeCommittedOffset(o))
	s.cache[string(key)] = o
	// Local commits advance foldedHWM to whatever the log now holds
	// (us): our cache is exactly that state. Only an external (replicated)
	// append can make Get re-fold.
	s.foldedHWM = s.inner.HighWatermark()
	s.commitsSinceCompact++
	// Compaction renumbers records — byte-incompatible with replicated
	// internal logs (plan 002 phase F completion). Only the keyless
	// (single-node) path compacts.
	if s.gate == nil || s.gate.Allowed() {
		if s.dir != "" && s.commitsSinceCompact >= s.compactEvery {
			if fl, ok := s.inner.(*log.FileLog); ok {
				if err := fl.Compact(); err != nil {
					return fmt.Errorf("firn/groups: compact offsets store: %w", err)
				}
				s.commitsSinceCompact = 0
			}
		}
	}
	return nil
}

// Compact refuses when the store is fenced (replicated internal log).
// Single-node callers may compact explicitly.
func (s *OffsetStore) Compact() error {
	if s.gate != nil && !s.gate.Allowed() {
		return fmt.Errorf("firn/groups: compact refused for replicated internal log __consumer_offsets")
	}
	if fl, ok := s.inner.(*log.FileLog); ok {
		if err := fl.Compact(); err != nil {
			return err
		}
		s.foldedHWM = -1
		return nil
	}
	return nil
}

// Get returns the folded committed offset for key. The fold is re-read
// only when the high watermark moved (local commit or replicated apply);
// the common hot path is O(1) against the cached fold.
func (s *OffsetStore) Get(key []byte) (CommittedOffset, bool) {
	s.refreshIfMoved()
	v, ok := s.cache[string(key)]
	return v, ok
}

// All returns the folded offsets for every key (latest per key).
func (s *OffsetStore) All() map[string]CommittedOffset {
	s.refreshIfMoved()
	out := make(map[string]CommittedOffset, len(s.cache))
	for k, v := range s.cache {
		out[k] = v
	}
	return out
}

// refreshIfMoved re-folds the log only when the high watermark changed.
// The fold replaces the cache (never merges), so replicated deletes via
// compaction (single-node path) cannot resurrect stale keys. On fold
// failure the last-known-good cache is kept.
func (s *OffsetStore) refreshIfMoved() {
	hw := s.inner.HighWatermark()
	if hw == s.foldedHWM {
		return
	}
	s.folds++
	fresh, err := fold(s.inner)
	if err != nil {
		return
	}
	s.cache = fresh
	s.foldedHWM = hw
}

// fold scans the log from the beginning and returns the latest value per key.
func fold(inner partitionLog) (map[string]CommittedOffset, error) {
	out := map[string]CommittedOffset{}
	hw := inner.HighWatermark()
	if hw == 0 {
		return out, nil
	}
	recs, err := inner.Fetch(0, int(hw))
	if err != nil {
		return nil, err
	}
	for _, r := range recs {
		v, err := decodeCommittedOffset(r.Value)
		if err != nil {
			continue
		}
		out[string(r.Key)] = v
	}
	return out, nil
}

// Close compacts (file-backed store) and closes the log.
func (s *OffsetStore) Close() error {
	fl, ok := s.inner.(*log.FileLog)
	if !ok {
		return nil
	}
	if s.sharedFl {
		// The broker owns the shared __consumer_offsets handle; the store
		// must never close or compact it (replication depends on stable
		// lifecycles). Nothing to release.
		return nil
	}
	if err := fl.Compact(); err != nil {
		return fmt.Errorf("firn/groups: compact offsets store: %w", err)
	}
	return fl.Close()
}
