package integration

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
)

// TailFileConnector is the stdlib reference source connector used to prove
// the checkpoint runtime (plan 007 slice 4) without external dependencies.
// It emits each complete line appended to a file as one record; the position
// is a byte offset. Rotation is NOT detected in v1 — the file is expected to
// be append-only (the common bridge case: app logs, NDJSON outboxes). A
// truncation (size < position) resets the position to 0 and re-reads,
// which is at-least-once safe.
type TailFileConnector struct {
	name string
	path string
}

// NewTailFileConnector creates a connector reading lines from path.
func NewTailFileConnector(name, path string) (*TailFileConnector, error) {
	if !ValidName(name) {
		return nil, fmt.Errorf("integration: invalid connector name %q", name)
	}
	if path == "" {
		return nil, fmt.Errorf("integration: tail source path required")
	}
	return &TailFileConnector{name: name, path: path}, nil
}

func (t *TailFileConnector) Name() string { return t.name }

func (t *TailFileConnector) Close() error { return nil }

// FetchBatch reads up to max complete lines beginning at the byte offset in
// position. A line without a trailing newline is not emitted until it is
// completed (the writer may still be appending).
func (t *TailFileConnector) FetchBatch(_ context.Context, position string, max int) ([]Record, error) {
	if max <= 0 {
		max = 500
	}
	var off int64
	if position != "" {
		v, err := strconv.ParseInt(position, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("integration: bad tail position %q: %w", position, err)
		}
		off = v
	}
	f, err := os.Open(t.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < off {
		off = 0 // truncated/rotated file: at-least-once re-read
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReaderSize(f, 64*1024)
	var recs []Record
	cursor := off
	for len(recs) < max {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				break // incomplete line: wait for the writer to finish it
			}
			trimmed := line[:len(line)-1]
			cursor += int64(len(line))
			if len(trimmed) == 0 {
				continue
			}
			v := append([]byte(nil), trimmed...)
			recs = append(recs, Record{Value: v, Position: strconv.FormatInt(cursor, 10)})
			continue
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return recs, nil
}
