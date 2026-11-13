// Package meta holds topic/partition metadata.
//
// Slice 3 (docs/plans/001-mvp-single-node.md): topic creation persists to a
// metadata JSON file (topics.json) via atomic rename, so topics survive a
// restart alongside their partition logs. The in-memory Registry stays
// available for tests and in-memory mode.
package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var (
	// ErrTopicExists is returned when creating a topic that already exists.
	ErrTopicExists = errors.New("firn/meta: topic already exists")
	// ErrTopicNotFound is returned when a topic does not exist.
	ErrTopicNotFound = errors.New("firn/meta: topic not found")
	// ErrInvalidPartitions is returned for partition counts < 1.
	ErrInvalidPartitions = errors.New("firn/meta: partitions must be >= 1")
	// ErrInvalidTopicMode is returned for unknown topic modes.
	ErrInvalidTopicMode = errors.New("firn/meta: mode must be stream or queue")
)

const (
	// ModeStream is the default: streaming topics (fetch by offset, replay).
	ModeStream = "stream"
	// ModeQueue is the work-queue mode: Pull/Ack with visibility timeout and DLQ.
	ModeQueue = "queue"
)

// Topic describes a topic, its partition count, and its mode.
type Topic struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Mode       string `json:"mode"`
}

// Registry is a concurrency-safe topic registry. With path set (via
// OpenRegistry) it is write-through: every Create is persisted to path
// before returning. With an empty path (NewRegistry) it is in-memory only.
type Registry struct {
	mu     sync.RWMutex
	topics map[string]Topic
	path   string
}

// NewRegistry returns an empty in-memory registry (nothing persisted).
func NewRegistry() *Registry { return &Registry{topics: map[string]Topic{}} }

// OpenRegistry loads (or creates) the topics file at path. A missing file is
// treated as empty; a corrupt or invalid file is a hard error — silently
// dropping topic metadata is never acceptable.
func OpenRegistry(path string) (*Registry, error) {
	r := &Registry{topics: map[string]Topic{}, path: path}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil // created on first Create
	}
	if err != nil {
		return nil, fmt.Errorf("firn/meta: read %s: %w", path, err)
	}
	var stored struct {
		Topics []Topic `json:"topics"`
	}
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, fmt.Errorf("firn/meta: parse %s: %w", path, err)
	}
	for _, t := range stored.Topics {
		if t.Name == "" {
			return nil, fmt.Errorf("firn/meta: %s: topic with empty name", path)
		}
		if t.Partitions < 1 {
			return nil, fmt.Errorf("firn/meta: %s: topic %q has %d partitions", path, t.Name, t.Partitions)
		}
		if t.Mode == "" {
			t.Mode = ModeStream // back-compat with pre-mode files
		}
		if t.Mode != ModeStream && t.Mode != ModeQueue {
			return nil, fmt.Errorf("firn/meta: %s: topic %q has invalid mode %q", path, t.Name, t.Mode)
		}
		if _, dup := r.topics[t.Name]; dup {
			return nil, fmt.Errorf("firn/meta: %s: duplicate topic %q", path, t.Name)
		}
		r.topics[t.Name] = t
	}
	return r, nil
}

// Create registers a new stream-mode topic.
func (r *Registry) Create(name string, partitions int) error {
	return r.CreateWithMode(name, partitions, ModeStream)
}

// CreateWithMode registers a new topic with the given mode ("stream" or
// "queue"). For a persistent registry the new state is flushed to disk
// atomically (tmp file + fsync + rename + dir fsync) before Create returns;
// on failure the in-memory state is unchanged.
func (r *Registry) CreateWithMode(name string, partitions int, mode string) error {
	if partitions < 1 {
		return ErrInvalidPartitions
	}
	if mode == "" {
		mode = ModeStream
	}
	if mode != ModeStream && mode != ModeQueue {
		return fmt.Errorf("%w: %q", ErrInvalidTopicMode, mode)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.topics[name]; ok {
		return fmt.Errorf("%w: %s", ErrTopicExists, name)
	}
	next := make(map[string]Topic, len(r.topics)+1)
	for k, v := range r.topics {
		next[k] = v
	}
	next[name] = Topic{Name: name, Partitions: partitions, Mode: mode}
	if r.path != "" {
		if err := persistTopics(r.path, next); err != nil {
			return fmt.Errorf("firn/meta: persist %s: %w", r.path, err)
		}
	}
	r.topics = next
	return nil
}

// persistTopics writes topics to path atomically: tmp file, fsync, rename,
// directory fsync.
func persistTopics(path string, topics map[string]Topic) error {
	out := make([]Topic, 0, len(topics))
	for _, t := range topics {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	data, err := json.MarshalIndent(struct {
		Topics []Topic `json:"topics"`
	}{Topics: out}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("firn/meta: create dir %s: %w", filepath.Dir(path), err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".topics-*.json.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// Get returns a topic by name.
func (r *Registry) Get(name string) (Topic, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.topics[name]
	if !ok {
		return Topic{}, fmt.Errorf("%w: %s", ErrTopicNotFound, name)
	}
	return t, nil
}

// List returns all topics sorted by name.
func (r *Registry) List() []Topic {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Topic, 0, len(r.topics))
	for _, t := range r.topics {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
