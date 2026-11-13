package log

// Frame decoding for replication (ADR-007 phase C). ParseFrames validates a
// raw frame byte string exactly the way recovery would (CRC per frame,
// length sanity) and returns the decoded frames in order. It is used by the
// ReplicaPut handler to validate frames before they are appended.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Frame is a decoded log frame.
type Frame struct {
	Offset int64
	Ts     int64
	Key    []byte
	Value  []byte
}

// ParseFrames decodes and CRC-validates all frames in raw. It returns an
// error on the first corrupt or truncated frame.
func ParseFrames(raw []byte) ([]Frame, error) {
	var out []Frame
	pos := 0
	for pos < len(raw) {
		if len(raw)-pos < 8 { // len prefix + crc
			return nil, errors.New("firn/log: truncated frame header")
		}
		bodyLen := binary.BigEndian.Uint32(raw[pos : pos+4])
		if bodyLen < 28 || bodyLen > 1<<28 {
			return nil, fmt.Errorf("firn/log: implausible frame body length %d", bodyLen)
		}
		end := pos + 4 + int(bodyLen)
		if end > len(raw) {
			return nil, errors.New("firn/log: truncated frame body")
		}
		buf := raw[pos+4 : end]
		body := buf[4:]
		if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(buf[0:4]) {
			return nil, fmt.Errorf("firn/log: crc mismatch at byte %d", pos)
		}
		offset := int64(binary.BigEndian.Uint64(body[0:8]))
		ts := int64(binary.BigEndian.Uint64(body[8:16]))
		keyLen := int(binary.BigEndian.Uint32(body[16:20]))
		valueLen := int(binary.BigEndian.Uint32(body[20:24]))
		if len(body) != 24+keyLen+valueLen {
			return nil, fmt.Errorf("firn/log: frame at byte %d has inconsistent lengths", pos)
		}
		out = append(out, Frame{
			Offset: offset,
			Ts:     ts,
			Key:    append([]byte(nil), body[24:24+keyLen]...),
			Value:  append([]byte(nil), body[24+keyLen:]...),
		})
		pos = end
	}
	return out, nil
}
