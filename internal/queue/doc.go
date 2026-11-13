// Package queue implements ack-based work-queue semantics on top of the
// same log used for streaming:
//
//   - acknowledge(deliveryID)   -> advance group position for that partition
//   - redelivery after visibility timeout expires
//   - dead-letter after maxDeliveries attempts
//
// Invariants: a message is visible to exactly one consumer per delivery,
// and an unacked message is redelivered at least once (at-least-once).
// Implementation lands after streaming slice 1 is solid.
package queue
