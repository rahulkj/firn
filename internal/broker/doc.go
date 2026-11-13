// Package broker wires the pieces together: partition assignment, the
// produce/fetch path, and (later) the queue ack/redelivery machinery.
//
// Implementation lands in docs/plans/001-mvp-single-node.md slice 2+.
// Keep this package thin: storage invariants live in internal/log,
// topic state in internal/meta, and this package orchestrates.
package broker
