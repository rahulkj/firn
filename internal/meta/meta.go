// Package meta holds topic/partition metadata.
//
// Slice 1 uses an in-memory registry. Persistence lands with the
// file-backed log (internal/log) so topics and their segment files
// can be created/recovered atomically. See docs/plans/001-mvp-single-node.md.
package meta

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	// ErrTopicExists is returned when creating a topic that already exists.
	ErrTopicExists = errors.New("eventing/meta: topic already exists")
	// ErrTopicNotFound is returned when a topic does not exist.
	ErrTopicNotFound = errors.New("eventing/meta: topic not found")
	// ErrInvalidPartitions is returned for partition counts < 1.
	ErrInvalidPartitions = errors.New("eventing/meta: partitions must be >= 1")
)

// Topic describes a topic and its partition count.
type Topic struct {
	Name       string
	Partitions int
}

// Registry is a concurrency-safe in-memory topic registry.
type Registry struct {
	mu     sync.RWMutex
	topics map[string]Topic
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{topics: map[string]Topic{}} }

// Create registers a new topic.
func (r *Registry) Create(name string, partitions int) error {
	if partitions < 1 {
		return ErrInvalidPartitions
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.topics[name]; ok {
		return fmt.Errorf("%w: %s", ErrTopicExists, name)
	}
	r.topics[name] = Topic{Name: name, Partitions: partitions}
	return nil
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
