package log

// Block descriptors (ADR-007, plan 002-ha phase A).
//
// A block is a sealed segment — immutable by construction and safe to treat
// as a unit of replication and GC. The descriptor is derived from the
// segment's rolling digest state (maintained incrementally, no extra I/O)
// and is deterministic across reopens: same input bytes, same descriptor ID.
//
// These descriptors are the substrate for ADR-007 phases B and C. Phase A
// emits them as local metadata only; nothing consumes them yet, and client
// behavior is unchanged.

import (
	"fmt"
	"hash/crc32"
	"time"
)

// BlockDescriptor describes one sealed block. IDs are content-addressed:
// ID = SHA-256 over the raw frame bytes in the block.
type BlockDescriptor struct {
	// ID is SHA-256 of the raw block bytes — the replication unit and the
	// content-addressed name of the block.
	ID [32]byte
	// BaseOffset is the segment base offset (the block's first offset).
	BaseOffset int64
	// FirstOffset and LastOffset bound the block's contents (FirstOffset ==
	// BaseOffset for non-empty blocks; LastOffset == BaseOffset+count-1).
	FirstOffset int64
	LastOffset  int64
	// Size is the block's on-disk size in bytes.
	Size int64
	// RecordCount is the number of frames in the block.
	RecordCount int64
	// CRCSet is the XOR of all frame CRCs — a cheap integrity fingerprint,
	// NOT a correctness mechanism (per-frame CRC is the real check).
	CRCSet uint32
	// SealedAt is derived from the newest record timestamp in the block —
	// stable across reopen.
	SealedAt time.Time
}

// Blocks returns descriptors for all sealed blocks in order. The active
// (unsealed) tail and empty blocks are excluded.
func (l *FileLog) Blocks() []BlockDescriptor {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []BlockDescriptor
	for _, seg := range l.segments {
		if !seg.sealed || seg.count == 0 {
			continue
		}
		if seg.blockCache == nil {
			sum := seg.digest.Sum(nil)
			d := BlockDescriptor{
				BaseOffset:  seg.baseOffset,
				FirstOffset: seg.firstOffset,
				LastOffset:  seg.lastOffset,
				Size:        seg.writePos,
				RecordCount: seg.count,
				CRCSet:      seg.crcXor,
				SealedAt:    time.UnixMilli(seg.maxTs),
			}
			copy(d.ID[:], sum)
			seg.blockCache = &d
		}
		out = append(out, *seg.blockCache)
	}
	return out
}

// feedSegmentDigest folds one raw frame (including the length prefix) into
// the segment's rolling digest state. Called on the write path and during
// recovery, so recovered blocks get identical IDs to freshly built ones.
func feedSegmentDigest(seg *segment, raw []byte, rec frameRecord) {
	_, _ = seg.digest.Write(raw)
	seg.crcXor ^= crc32.ChecksumIEEE(raw[8:])
	if seg.firstOffset == -1 {
		seg.firstOffset = rec.Offset
	}
	seg.lastOffset = rec.Offset
	seg.count++
}

// String renders the descriptor for diagnostics.
func (d BlockDescriptor) String() string {
	return fmt.Sprintf("block[%d..%d] n=%d size=%d id=%x", d.FirstOffset, d.LastOffset, d.RecordCount, d.Size, d.ID[:8])
}
