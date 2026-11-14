# ADR-001: Scope — what firn is and is not

Status: accepted · 2026-11-13 · delivered (v0.1.0)

## Context

The goal: a Go broker providing both **streaming** (topics, partitions, replay, consumer groups) and **queue** (acknowledgements, redelivery, dead-lettering) capabilities — one storage engine, two consumption semantics. The naive reading — "reimplement both classic broker families at once" — is years of work and mostly protocol archaeology.

## Decision

Firn is a **single, unified log-based broker**:

- Streaming mode: topics with N partitions; records fetched by offset; consumer groups commit offsets.
- Queue mode: the same log, consumed via `acknowledge()`; redelivery after a visibility timeout; DLQ after `maxDeliveries`.

One storage engine, one replication story (later), two API surfaces.

### Explicit non-goals for v1

Replication beyond one cluster; exactly-once semantics/transactions; wire compatibility with any other broker; multi-tenancy/authZ; multi-datacenter; stream processing. Each may be revisited as its own ADR.

## Consequences

- We optimize for correctness and operability of one storage engine, not protocol breadth.
- Client compatibility comes from SDKs we write, not wire compatibility.
- "Streaming AND queue" in v1 means *semantics* parity within one deployment, not two brokers in a trench coat.
- Replication remains a non-goal for the MVP (v0.1.0). It is explicitly scoped for the v-next milestone by [ADR-007](007-scaled-deployment.md); that ADR defines HA, cross-node replication, election, security, and admin UI.

## Implementation notes (2026-11-13): client SDKs

ADR-001 says client compatibility comes from SDKs we write, not wire compatibility. No client SDK has shipped — `firn-cli` remains the reference client (and is not a library). SDK extraction is unscheduled as of this date. Consumers hand-roll gRPC calls or use `firn-cli` for ops paths; a Go SDK (proto wrapper + connection pooling + auth helpers) remains a valid future milestone and was scoped here but never built.

## Addendum (2026-11-13): Go SDK delivered

ADR-001 named client SDKs as the compatibility vehicle. As of v0.4.0 only Go exists — `sdk/firngo` (Connect, TLS or plaintext for dev, produce/fetch/subscribe with auth). Python/Java/.NET SDKs remain unbuilt; consumers use `firn-cli` or proto-generated stubs. The register of all client-facing gaps lives in ADR-013.
