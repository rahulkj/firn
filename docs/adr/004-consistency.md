# ADR-004: Consistency & replication stance for v1

Status: accepted · 2026-11-13 · delivered (v0.1.0)

## Decision

- v1 is **single-node**: durability = local disk + fsync; availability = not replicated.
- Produce path: `acks` levels are deliberately minimal:
  - `acks=1` (v1 default): append fsync'd on the single node before ack.
  - `acks=all`: identical to `acks=1` in v1 — documented, not faked. Real quorum arrives with replication.
- Consumer groups: offsets committed in an internal compacted topic (`__consumer_offsets`) in slice 4.
- Replication is designed for but not built: the log is segmented, offset-addressed, and segment-deletable — all replication-friendly properties. (ADR-006 ended up being used for metrics; the replication decision is **ADR-007**.)

## Consequences

- No split-brain in v1; no false quorum semantics.
- Honest upgrade path: replication slots in as an ADR (see ADR-007) without changing the single-node API.
