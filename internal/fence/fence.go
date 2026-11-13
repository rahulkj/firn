// Package fence provides a cheap write fence for single-writer internal
// topics (__consumer_offsets, __queue_attempts, __audit, __api_keys).
//
// In cluster mode an internal topic may be appended by exactly one node:
// the holder of its internal write lease (see replication.Advisor). A fence
// is a pure predicate — it reads the quorum's lease table, never the disk —
// so it adds no measurable cost to the write hot path.
//
// GateBox is late-bound: the broker hands it to internal stores at
// construction time; the daemon fills it in after the quorum starts.
// An unset box (single-node mode, in-memory mode, tests) allows all writes.
package fence

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// ErrFenced is returned (wrapped) when a write to an internal topic is
// rejected because this node does not hold its lease. Callers treat it as
// retryable after re-resolving the holder.
var ErrFenced = errors.New("slipway/fence: internal topic fenced on this node")

// Gate reports whether a write to the given internal topic (partition 0)
// is allowed on this node. A nil Gate allows everything.
type Gate func(topic string) error

// GateBox is a late-bound Gate holder. Stores keep a *GateBox and read it on
// every gated write; once set, the gate applies from the very next write.
type GateBox struct{ v atomic.Value } // holds Gate

// NewGateBox returns an empty box (all writes allowed until Set).
func NewGateBox() *GateBox { return &GateBox{} }

// Set installs the gate. Subsequent Allow calls consult it.
func (b *GateBox) Set(g Gate) { b.v.Store(g) }

// Allowed reports whether writes are currently ungated (box empty).
func (b *GateBox) Allowed() bool { _, ok := b.v.Load().(Gate); return !ok }

// Allow permits the write when the gate allows it, wraps fence errors with
// actionable context, and returns nil when no gate is set.
func (b *GateBox) Allow(topic string) error {
	if b == nil {
		return nil
	}
	g, ok := b.v.Load().(Gate)
	if !ok || g == nil {
		return nil
	}
	if err := g(topic); err != nil {
		if errors.Is(err, ErrFenced) {
			return err
		}
		return fmt.Errorf("%w: %v", ErrFenced, err)
	}
	return nil
}
