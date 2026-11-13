package federation

// Frame re-encoding helpers for serving FILTERED frame windows to peers.
// internal/log is not modified; the format is re-implemented here at the
// federation seam (same layout as internal/log/filelog.go's encodeFrame).

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/rahulkj/firn/internal/log"
)

// encodeFrame encodes one frame in the on-disk layout.
func encodeFrame(offset, ts int64, key, value []byte) []byte {
	keyLen, valueLen := len(key), len(value)
	bodyLen := 4 + 8 + 8 + 4 + 4 + keyLen + valueLen
	buf := make([]byte, 4+bodyLen)

	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))
	body := buf[8:]
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

// ExportFramesWithout encodes the given frames, dropping any whose offset
// lies inside skipRanges. Frames arrive in offset order (ReadRange output);
// the result preserves that order with ORIGINAL offsets/timestamps — the
// caller decides what to do with them (re-origin on import).
func ExportFramesWithout(frames []log.Frame, skipRanges []LocalRange) []byte {
	out := []byte{}
	for _, f := range frames {
		if RangeContains(skipRanges, f.Offset) {
			continue // foreign-imported by the requesting peer: echo, skip
		}
		out = append(out, encodeFrame(f.Offset, f.Ts, f.Key, f.Value)...)
	}
	return out
}
