# ADR-004: Consistency & replication stance for v1

Status: accepted · 2026-11-13 · delivered (v0.1.0) · amended 2026-11-14 (v0.5.0: replication shipped GA)

## Decision (as delivered in v0.1.0)

- v1 is **single-node**: durability = local disk + fsync; availability = not replicated.
- Produce path: `acks` levels are deliberately minimal:
  - `acks=1` (v1 default): append fsync'd on the single node before ack.
  - `acks=all`: identical to `acks=1` in v1 — documented, not faked. Real quorum arrives with replication.
- Consumer groups: offsets committed in an internal compacted topic (`__consumer_offsets`) in slice 4.
- Replication is designed for but not built at v0.1.0: the log is segmented, offset-addressed, and segment-deletable — all replication-friendly properties. (ADR-006 ended up being used for metrics; the replication decision is **ADR-007**.)

## Addendum (v0.5.0, 2026-11-14): replication is delivered, `acks` is not

The `acks=1`/`acks=all` distinction in this ADR was aspirational v1 vocabulary; no acks knob exists in the code or proto, and none is planned for v0.5.0. What actually shipped:

- Durability per-partition (ADR-007, v0.5.0 GA): a block is committed via quorum (`EntryBlockCommit`) once a majority holds it — replication composed with the local fsync model, not a rename of it.
- Local produce acknowledgements are unchanged: an fsync on the caller's partition log (or, with `-flush-window`, that of the covering window). A produce is acknowledged before its block is replicated — the at-least-once, unsealed-tail window documented in ADR-007/005 applies on top.
- Committed offsets in `__consumer_offsets`, monotonic unless reset — unchanged.
- Exactly-once remains out of scope — unchanged.
- Honest naming: there is no `acks=1` vs `acks=all` delivery contract to compare against other brokers' flag names; compare against durability windows and fence semantics, not against a nonexistent API surface.

## Consequences (v0.1.0)

- No split-brain in v1; no false quorum semantics.
- Honest upgrade path: replication slots in as an ADR (see ADR-007) without changing the single-node API.
