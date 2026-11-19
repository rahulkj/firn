// Package integration implements the shared contract for Firn's ingress
// integrations (ADR-016, docs/plans/007-ingress-integrations.md): source
// connectors, durable checkpoints, and dead-letter routing. Everything here
// runs OUTSIDE firnd and talks to Firn only through the client API — no
// storage-path code lives in this package.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Record is one unit of work moving source -> Firn. Position is an opaque
// source cursor (e.g. a file offset, a Kafka offset tuple, a CDC LSN) that
// the connector hands back out; the runtime persists it via CheckpointStore
// only after the produce to Firn has been acknowledged.
type Record struct {
	Key      []byte
	Value    []byte
	Headers  map[string]string
	Position string
}

// SourceConnector is implemented by every source connector (polling or
// push-with-buffer). Implementations must be safe for sequential use by one
// Runner goroutine; concurrency inside a connector is its own concern.
type SourceConnector interface {
	// Name identifies this connector instance; it is the checkpoint key.
	Name() string
	// FetchBatch reads at most max records after position. An empty
	// position means "start from the source's initial position". A nil
	// slice with a nil error means "no data right now" (caller backs off).
	FetchBatch(ctx context.Context, position string, max int) ([]Record, error)
	// Close releases source resources. FetchBatch must not be called after.
	Close() error
}

// CheckpointStore persists per-connector positions. Implementations must be
// crash-safe: a crash between Write and read-back must never surface a torn
// position.
type CheckpointStore interface {
	Load(ctx context.Context, connector string) (string, error)
	Store(ctx context.Context, connector, position string) error
}

// FileCheckpointStore keeps positions in one JSON object file with atomic
// rename and 0600/0700 perms. Suitable for single-instance connector
// processes (the common deployment for bridges). For multi-instance
// coordination, front connectors with a leader lease or use the Firn-topic
// checkpoint store (see TopicCheckpointStore) instead.
type FileCheckpointStore struct {
	path string
	mu   sync.Mutex
}

// NewFileCheckpointStore opens (creating if needed) the checkpoint file at path.
func NewFileCheckpointStore(path string) (*FileCheckpointStore, error) {
	if path == "" {
		return nil, errors.New("integration: checkpoint path required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("integration: checkpoint dir: %w", err)
	}
	s, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("integration: read checkpoint: %w", err)
		}
	} else {
		var m map[string]string
		if err := json.Unmarshal(s, &m); err != nil {
			return nil, fmt.Errorf("integration: corrupt checkpoint file %s: %w", path, err)
		}
		if err := os.Chmod(path, 0o600); err != nil {
			return nil, fmt.Errorf("integration: chmod checkpoint: %w", err)
		}
	}
	return &FileCheckpointStore{path: path}, nil
}

func (f *FileCheckpointStore) Load(_ context.Context, connector string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("integration: read checkpoint: %w", err)
	}
	var m map[string]string
	if err := json.Unmarshal(s, &m); err != nil {
		return "", fmt.Errorf("integration: corrupt checkpoint file %s: %w", f.path, err)
	}
	return m[connector], nil
}

func (f *FileCheckpointStore) Store(_ context.Context, connector, position string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := map[string]string{}
	if s, err := os.ReadFile(f.path); err == nil {
		if err := json.Unmarshal(s, &m); err != nil {
			return fmt.Errorf("integration: corrupt checkpoint file %s: %w", f.path, err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("integration: read checkpoint: %w", err)
	}
	m[connector] = position
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o600); err != nil {
		return fmt.Errorf("integration: write checkpoint tmp: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("integration: rename checkpoint: %w", err)
	}
	return nil
}

// ErrTerminallyBroken marks a record that must be dead-lettered rather than
// retried (mapping failure, validation failure, oversized payload).
var ErrTerminallyBroken = errors.New("integration: terminal record failure")

// TerminalError wraps ErrTerminallyBroken with detail for the DLQ payload.
func TerminalError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrTerminallyBroken, fmt.Sprintf(format, args...))
}

// IsTerminal reports whether err is a terminal (dead-letter) error.
func IsTerminal(err error) bool {
	return errors.Is(err, ErrTerminallyBroken)
}

// DLQPayload is the structured JSON body written to the dead-letter topic.
// Consumers can reprocess by reading Source coordinates.
type DLQPayload struct {
	Connector   string            `json:"connector"`
	Stage       string            `json:"stage"` // fetch | map | produce | validate
	Position    string            `json:"position,omitempty"`
	Error       string            `json:"error"`
	Headers     map[string]string `json:"headers,omitempty"`
	ValueBase64 string            `json:"value_base64,omitempty"`
}

// ValidName reports whether s is a safe connector/route name for use in
// checkpoints, keys, and logs (no whitespace, no '/', reasonable charset).
func ValidName(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for _, r := range s {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.'
		if !ok {
			return false
		}
	}
	return true
}

// JoinKey builds a checkpoint/DLQ key from parts with a separator that
// never appears in ValidName output.
func JoinKey(parts ...string) string {
	return strings.Join(parts, "\x00")
}
