package log

// Slice 4 (docs/plans/001-mvp-single-node.md): log compaction.
//
// Compact folds the log to last-value-per-key and rewrites it densely.
// Unlike Kafka's sparse retention, the surviving records are renumbered
// 0..M-1 because this repo's recovery path assumes dense offsets from the
// (possibly non-zero) retention horizon — compaction is used for internal
// stores (e.g. __consumer_offsets) whose raw record offsets are not part
// of any protocol, so renumbering loses nothing.
//
// The rewrite is crash-safe:
//
//	1. write the folded log to <dir>.compact (fully synced)
//	2. rename <dir> -> <dir>.old
//	3. rename <dir>.compact -> <dir>
//	4. fsync the parent directory
//	5. delete <dir>.old (a crash here leaves a harmless leftover,
//	   cleaned up on the next Open)
//
// Open resolves crash windows: if the live directory is missing it falls
// back to .compact (preferred) or .old. A .compact next to an existing live
// directory means an interrupted compact whose output was superseded — it
// is left in place (never silently deleted) and reported as an error.
//
// Compact is serialized against other methods by the log's internal mutex;
// the rewrite itself happens on disk before the in-memory state is swapped.
// After Compact returns, the log remains usable.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type compactEntry struct {
	ts    int64
	key   []byte
	value []byte
}

// Compact rewrites the log keeping only the last record per key. It returns
// the number of records dropped.
func (l *FileLog) Compact() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Single pass: collect all records in offset order.
	type elem struct {
		offset int64
		ts     int64
		key    []byte
		value  []byte
	}
	var all []elem
	for _, seg := range l.segments {
		pos := int64(0)
		for pos < seg.writePos {
			rec, raw, ok, err := readFrameAt(seg.file, pos)
			if err != nil {
				return fmt.Errorf("eventing/log: compact scan %s: %w", seg.path, err)
			}
			if !ok {
				return fmt.Errorf("eventing/log: compact scan %s at %d: corrupt frame", seg.path, pos)
			}
			all = append(all, elem{offset: rec.Offset, ts: rec.Ts, key: rec.Key, value: rec.Value})
			pos += int64(len(raw))
		}
	}

	// Last occurrence per key wins (iteration is already in offset order).
	type e2 struct {
		offset int64
		e      compactEntry
	}
	latest2 := make(map[string]e2, len(all))
	for _, e := range all {
		latest2[string(e.key)] = e2{offset: e.offset, e: compactEntry{ts: e.ts, key: e.key, value: e.value}}
	}
	survivors := make([]e2, 0, len(latest2))
	for _, e := range latest2 {
		survivors = append(survivors, e)
	}
	sort.Slice(survivors, func(i, j int) bool { return survivors[i].offset < survivors[j].offset })

	compactDir := l.dir + ".compact"
	if err := os.RemoveAll(compactDir); err != nil {
		return fmt.Errorf("eventing/log: remove stale %s: %w", compactDir, err)
	}
	nf, err := Open(compactDir, WithSyncOnAppend(true), WithMaxSegmentBytes(l.maxSegmentBytes))
	if err != nil {
		return fmt.Errorf("eventing/log: create %s: %w", compactDir, err)
	}
	for _, k := range survivors {
		nf.appendWithTimestamp(k.e.key, k.e.value, k.e.ts)
	}
	if err := nf.Close(); err != nil {
		_ = os.RemoveAll(compactDir)
		return fmt.Errorf("eventing/log: sync %s: %w", compactDir, err)
	}

	// Swap directories: close current segments first so the rename never
	// races open file handles on unusual filesystems.
	for _, seg := range l.segments {
		if err := seg.file.Sync(); err != nil {
			return fmt.Errorf("eventing/log: sync before compact swap %s: %w", seg.path, err)
		}
		if err := seg.file.Close(); err != nil {
			return fmt.Errorf("eventing/log: close before compact swap %s: %w", seg.path, err)
		}
	}
	oldDir := l.dir + ".old"
	_ = os.RemoveAll(oldDir) // stale leftover from an earlier interrupted compact
	if err := os.Rename(l.dir, oldDir); err != nil {
		return fmt.Errorf("eventing/log: rename %s -> %s: %w", l.dir, oldDir, err)
	}
	if err := syncParent(l.dir); err != nil {
		return err
	}
	if err := os.Rename(compactDir, l.dir); err != nil {
		// Live dir is missing; restore from .old so we never lose data.
		_ = os.Rename(oldDir, l.dir)
		return fmt.Errorf("eventing/log: rename %s -> %s: %w", compactDir, l.dir, err)
	}
	if err := syncParent(l.dir); err != nil {
		return err
	}
	if err := os.RemoveAll(oldDir); err != nil {
		return fmt.Errorf("eventing/log: remove %s: %w", oldDir, err)
	}

	// Reopen the rewritten log and adopt its state.
	nl, err := Open(l.dir, WithSyncOnAppend(l.syncOnAppend), WithMaxSegmentBytes(l.maxSegmentBytes))
	if err != nil {
		return fmt.Errorf("eventing/log: reopen after compact %s: %w", l.dir, err)
	}
	l.segments = nl.segments
	l.hwm = nl.hwm
	return nil
}

// resolveCompactDirs repairs crash windows left by Compact and is called at
// the top of Open. It never silently deletes data:
//   - dir missing, .compact present  -> .compact becomes dir
//   - dir missing, only .old present -> .old becomes dir
//   - dir present, .old present      -> remove .old (dir is authoritative)
//   - dir present, .compact present  -> leave both in place, error
func resolveCompactDirs(dir string) error {
	compact := dir + ".compact"
	old := dir + ".old"

	_, dirErr := os.Stat(dir)
	_, cErr := os.Stat(compact)
	_, oErr := os.Stat(old)
	dirMissing := errors.Is(dirErr, os.ErrNotExist)
	compactOK := cErr == nil
	oldOK := oErr == nil

	switch {
	case dirMissing && compactOK:
		if err := os.Rename(compact, dir); err != nil {
			return fmt.Errorf("eventing/log: restore %s from .compact: %w", dir, err)
		}
		if oldOK {
			_ = os.RemoveAll(old) // superseded by the compact output
		}
		return syncParent(dir)
	case dirMissing && !compactOK && oldOK:
		if err := os.Rename(old, dir); err != nil {
			return fmt.Errorf("eventing/log: restore %s from .old: %w", dir, err)
		}
		return syncParent(dir)
	case !dirMissing && oldOK:
		if err := os.RemoveAll(old); err != nil {
			return fmt.Errorf("eventing/log: remove stale %s: %w", old, err)
		}
	case !dirMissing && compactOK:
		return fmt.Errorf("eventing/log: interrupted compact left %s alongside live %s: refusing to delete it; inspect and remove manually", compact, dir)
	}
	return nil
}

func syncParent(path string) error {
	parent := filepath.Dir(path)
	d, err := os.Open(parent)
	if err != nil {
		return fmt.Errorf("eventing/log: open parent dir %s: %w", parent, err)
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return fmt.Errorf("eventing/log: fsync parent dir %s: %w", parent, syncErr)
	}
	return closeErr
}
