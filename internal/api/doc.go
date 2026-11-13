// Package api exposes eventing over the network: gRPC + HTTP admin/health.
//
// Design decision (docs/adr/002-api.md): a clean, minimal API first.
// Kafka wire compatibility is a non-goal for v1 and may arrive later
// as a protocol gateway, never inside the broker core.
package api
