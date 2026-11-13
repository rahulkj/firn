package quorum

// Persistence for the metadata quorum. Same discipline as internal/log:
// append-only, CRC-validated frames; torn tail truncated on open.
//
// Two artifacts live in the node's directory:
//
//	hardstate.json  { currentTerm, votedFor }  — atomic tmp+fsync+rename+dir sync
//	log.bin         [len u32][crc u32][json Entry] — append-only, crc covers payload

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

type hardState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
	CommitIndex uint64 `json:"commit_index"`
}

func hardStatePath(dir string) string { return filepath.Join(dir, "hardstate.json") }
func logPath(dir string) string       { return filepath.Join(dir, "log.bin") }

// writeHardState atomically persists the hard state.
func writeHardState(dir string, hs hardState) error {
	data, err := json.Marshal(hs)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".hardstate-*.json")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), hardStatePath(dir)); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return fmt.Errorf("slipway/quorum: fsync dir %s: %w", dir, syncErr)
	}
	return closeErr
}

// readHardState loads the hard state, or returns the zero state if missing.
func readHardState(dir string) (hardState, error) {
	var hs hardState
	data, err := os.ReadFile(hardStatePath(dir))
	if errors.Is(err, os.ErrNotExist) {
		return hardState{}, nil
	}
	if err != nil {
		return hs, fmt.Errorf("slipway/quorum: read hard state: %w", err)
	}
	if err := json.Unmarshal(data, &hs); err != nil {
		return hs, fmt.Errorf("slipway/quorum: parse hard state: %w", err)
	}
	return hs, nil
}

// appendLogEntry appends one entry to the metadata log with fsync.
func appendLogEntry(dir string, e Entry) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)

	f, err := os.OpenFile(logPath(dir), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("slipway/quorum: open log: %w", err)
	}
	n, err := f.Write(frame)
	if err != nil || n != len(frame) {
		_ = f.Close()
		return fmt.Errorf("slipway/quorum: append log: %w (%d/%d bytes)", err, n, len(frame))
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("slipway/quorum: sync log: %w", err)
	}
	return f.Close()
}

// recoverLog scans the metadata log, validates CRC per frame, truncates any
// torn tail, and returns the recovered entries.
func recoverLog(dir string) ([]Entry, int64, error) {
	path := logPath(dir)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, 0, fmt.Errorf("slipway/quorum: open log: %w", err)
	}
	defer func() { _ = f.Close() }()

	var entries []Entry
	pos := int64(0)
	for {
		var hdr [8]byte
		if _, err := f.ReadAt(hdr[:], pos); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, 0, err
		}
		payloadLen := binary.BigEndian.Uint32(hdr[0:4])
		crc := binary.BigEndian.Uint32(hdr[4:8])
		if payloadLen > 1<<26 { // 64 MiB sanity cap
			break // torn/garbage frame
		}
		buf := make([]byte, payloadLen)
		if _, err := f.ReadAt(buf, pos+8); err != nil {
			break // torn tail
		}
		if crc32.ChecksumIEEE(buf) != crc {
			break // CRC mismatch: torn tail
		}
		var e Entry
		if err := json.Unmarshal(buf, &e); err != nil {
			break
		}
		entries = append(entries, e)
		pos += int64(8 + payloadLen)
	}
	// Truncate any torn tail.
	st, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	if st.Size() > pos {
		if err := f.Truncate(pos); err != nil {
			return nil, 0, fmt.Errorf("slipway/quorum: truncate torn tail %s: %w", path, err)
		}
		if err := f.Sync(); err != nil {
			return nil, 0, fmt.Errorf("slipway/quorum: sync %s after truncation: %w", path, err)
		}
	}
	return entries, pos, nil
}

// rewriteLog rewrites the metadata log with the given entries (used when
// truncating conflicting uncommitted entries on a follower).
func rewriteLog(dir string, entries []Entry) error {
	path := logPath(dir)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("slipway/quorum: rewrite log: %w", err)
	}
	for _, e := range entries {
		payload, err := json.Marshal(e)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
		frame := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
		copy(frame[8:], payload)
		if _, err := f.Write(frame); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("slipway/quorum: rewrite log: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("slipway/quorum: sync rewritten log: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("slipway/quorum: close rewritten log: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("slipway/quorum: rename rewritten log: %w", err)
	}
	return nil
}

// recover loads hard state and the metadata log. Only entries covered by the
// persisted commit index are applied; the rest of the tail is re-committed
// through normal replication (uncommitted entries from a dead term may be
// truncated by the next leader, so exposing them would violate safety).
func (n *Node) recover() error {
	hs, err := readHardState(n.dir)
	if err != nil {
		return err
	}
	entries, size, err := recoverLog(n.dir)
	if err != nil {
		return err
	}
	n.currentTerm = hs.CurrentTerm
	n.votedFor = hs.VotedFor
	n.commitIndex = hs.CommitIndex
	n.lastApplied = 0
	n.log = entries
	n.logBytes = size
	for n.lastApplied < n.commitIndex && int(n.lastApplied) < len(n.log) {
		n.lastApplied++
		applyEntry(n, n.log[n.lastApplied-1])
	}
	return nil
}

func applyEntry(n *Node, e Entry) {
	switch e.Type {
	case EntryNoop:
	case EntryTopicCreate:
		var t TopicMetadata
		if err := json.Unmarshal(e.Data, &t); err == nil && t.Name != "" {
			n.topics[t.Name] = t
		}
	case EntryBlockCommit:
		var b BlockCommit
		if err := json.Unmarshal(e.Data, &b); err == nil && b.Topic != "" {
			k := blockKey(b.Topic, b.Partition)
			n.blocks[k] = append(n.blocks[k], b)
		}
	case EntryLeaseGrant:
		var l Lease
		if err := json.Unmarshal(e.Data, &l); err == nil && l.Topic != "" {
			k := blockKey(l.Topic, l.Partition)
			cur, ok := n.leases[k]
			if !ok || l.Epoch > cur.Epoch {
				n.leases[k] = l
			}
		}
	}
	if n.applyHook != nil {
		n.applyHook(e)
	}
}
