// Package ratelimit implements a simple per-key token bucket used at the
// broker's edge (ADR-002: not at the storage core).
package ratelimit

import (
	"sync"
	"time"
)

// Bucket is a token bucket keyed by an opaque string (usually the key hash).
type Bucket struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	capacity float64
	rate     float64 // tokens/second
}

type bucket struct {
	tokens    float64
	lastCheck time.Time
}

// New returns a bucket with capacity cap tokens, refilling at rate tokens/sec.
func New(cap, rate float64) *Bucket {
	return &Bucket{buckets: map[string]*bucket{}, capacity: cap, rate: rate}
}

// Allow reports whether key may proceed; consumes one token if allowed.
func (b *Bucket) Allow(key string) bool {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	bk, ok := b.buckets[key]
	if !ok {
		bk = &bucket{tokens: b.capacity, lastCheck: now}
		b.buckets[key] = bk
	}
	elapsed := now.Sub(bk.lastCheck).Seconds()
	bk.tokens += elapsed * b.rate
	if bk.tokens > b.capacity {
		bk.tokens = b.capacity
	}
	bk.lastCheck = now
	if bk.tokens >= 1 {
		bk.tokens -= 1
		return true
	}
	return false
}

// SlidingWindow is a strict per-key sliding-window counter for anonymous
// surfaces (healthz probes, refused-auth attempts). It is cheap on the hot
// path — a single map and a single mutex per process.
type SlidingWindow struct {
	mu      sync.Mutex
	buckets map[string][]time.Time
	window  time.Duration
	max     int
}

// NewSlidingWindow allows at most max hits per window per key.
func NewSlidingWindow(max int, window time.Duration) *SlidingWindow {
	return &SlidingWindow{buckets: map[string][]time.Time{}, window: window, max: max}
}

// Allow records one hit for key and reports whether it fits in the window.
func (w *SlidingWindow) Allow(key string) bool {
	now := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()
	cutoff := now.Add(-w.window)
	kept := make([]time.Time, 0, len(w.buckets[key])+1)
	for _, t := range w.buckets[key] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	if len(kept) >= w.max {
		w.buckets[key] = kept
		return false
	}
	w.buckets[key] = append(kept, now)
	return true
}
