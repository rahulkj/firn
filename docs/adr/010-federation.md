# ADR-010: Federation — inter-cluster event replication

Status: accepted · 2026-11-13 · Implementation proceeds via `docs/plans/004-federation.md`. Storage-adjacent review gates per slice; nothing here is committed until reviewed.

## Context

ADR-007 scaled one cluster across nodes. It did not address multiple, independent Firn clusters: separate quorums, separate logs, separate operators (or the same operator with disjoint failure domains — dev/staging/prod, regional hubs, a DR site, an edge↔cloud pair). Customers asked for:

1. **Full federation** — clusters can both read and both write the same topic or queue, with data flowing in both directions.
2. **One-way federation** — one cluster streams a topic/queue to another for DR, cache warming, or read scaling.
3. **Selective replication** — per-topic and per-queue policies, not all-or-nothing.
4. Clients must **see federated progress** and keep their positions across it.

The v1 stakes are high and the failure mode of getting this wrong (silent cross-cluster loss or amplification) is worse than not having the feature. This ADR records the decisions that make it bounded and honest.

## Decision summary

Federation between two independent Firn clusters is an **explicit, operator-registered, mutually configured** relationship driven by:

1. **API-key registration** — each side creates a federation-scoped key and hands it to the other side out-of-band. No implicit trust, no auto-discovery.
2. **A policy object** per peer (direction, mode, selectors, partition requirements) stored in the cluster-local internal topic `__federation_peers`.
3. **A pull-based data plane** — the destination pulls from the source over the same mTLS + bearer channel the admin API uses today (`ReplicaFetch`-shaped RPC over `FederationService`). Push is intentionally out of scope for v1. **No inbound port exposure required on either side.**
4. Two modes, per topic/partition scope:

| Mode | Writer set | Import encoding | Offset space | Ordering |
|---|---|---|---|---|
| `passive` (one-way DR) | source only | byte-identical sealed blocks (HWM preserved) | identical to source | identical to source |
| `active-active` (full) | **both** | frame re-origin through the normal append path | independent per cluster | **no global order** |

5. **Content-hash / cursor dedup** on every pull range: each cluster imports only frames it has never seen, tracked in a persisted, idempotent-internal-log cursor (`__federation_cursors`). The cursor additionally records which local offset ranges are foreign-applied and from which origin cluster — that record is the echo filter: a frame that was itself imported from cluster X is never re-imported from X. This is sufficient; frames need no new headers.
6. **No frame-format change, no storage-invariant change.** Re-origin frames go through the existing append path; offsets stay monotonic per partition per cluster; the HWM rule holds; torn-tail recovery is the existing code path.
7. **Client position visibility**: `FetchResponse` gains `federation_hwm` (the contiguous offset applied from every configured peer for this partition, min across peers). Per-range detail is exposed via `GET /federation/cursors` and `firn federation status`. The internal `__federation_cursors` log is the source of truth; the annotation is a hint, never a correctness input.
8. **Queues federate fully** in v1 — payload and `__queue_attempts` (attempts/acks/DLQ state) replicate both directions via the same frame machinery. Delivery engine remains active on every cluster; convergence is documented as at-least-once-per-cluster + eventually-consistent once both clusters see the same attempt view. Cross-federation exactly-once consumption is **out of scope**.

## Federation is not replication (ADR-007 vs ADR-010)

| Axis | ADR-007 (intra-cluster) | ADR-010 (inter-cluster) |
|---|---|---|
| Failure domain | one quorum; replicated byte-identical | independent quorums; may not even agree on config |
| Federation unit | sealed block (content address) | sealed block (passive) or single frame (active-active) |
| Transport | same listener, in-cluster mesh | mTLS + federation-scoped bearer, possibly across networks/NAT |
| Auth | internal mesh, no API keys | **API keys** |
| Offset identity | preserved (single HWM) | preserved (passive) / re-origined per cluster (active-active) |
| Client position | same offsets everywhere | see `__federation_cursors` / `federation_hwm` |

## Registration and identity

- `POST /federation/keys` (`role: federation`) → plaintext shown once, stored in `__api_keys` like any key; role cannot produce/fetch/ack on normal topics (federation role only opens the federation RPCs).
- `POST /federation/peers` registers one direction of one peer with its addr + CA + key + policy. Both clusters must explicitly register; there is no handshake auto-negotiation.
- `GET /federation/peers`, `DELETE /federation/peers/{name}`, `GET /federation/cursors`.
- Every mutation is audited (`__audit`), like every other admin action.
- Policy validation at registration: direction, mode, partition count match, target topics/queues exist (or policy allows auto-create with matching partition count), peer addr is reachable (online check, non-fatal warning otherwise).
- Secrets never leave the registering side; peers hold *their* keys only, and the source holds no inbound credentials for the destination.

## Data plane mechanics

### Passive (one-way) mode

- Sink policy enforces **sink write fence** for federated topics (produce → HTTP 409 / gRPC FAILED_PRECONDITION with `federated: passive`).
- Sink puller (interval `-federation-interval`, default 500ms, batch max 1 MiB) calls `FederationFetch(source topic, partition, first_offset, last_offset)` with source offsets preserved; bytes are appended byte-identically via the existing replicated-apply path; source HWM is respected.
- Queues in passive mode are **mirror-only**: `__queue_attempts` (attempts/acks/DLQ state) is mirrored byte-identically; the sink does not deliver from the mirror until cutover. Cutover is an operator action (flip policy mode or activate the sink), documented as a runbook step.

### Active-active (full) mode

- Both clusters accept writes at any time to the federated topics/queues.
- A puller on each side (per peer, partition-aligned): read cursor → request `[cursor.last_source_offset, source_hwm]` frames from the source → re-encode each frame with the next local offset (key, value, ts preserved; offsets re-assigned) → append → upsert cursor `{origin: "remote", last_source_offset, last_local_offset}`.
- **Echo prevention by invariant**: cursor `{origin: "remote", peer: X, last_local_offset}` tells this cluster "local records at [start, cursor.last_local_offset] came from X". When importing from X, skip nothing less than cursor, and import *everything* newer; when importing from peer Y it is symmetric. The resending of a frame originally created on local cluster A through A→B→A is filtered by A's cursor for `(B, A-origin ranges)`, not by headers.
- **Ordering**: records from the local writer and the remote writer are appended in import order; consumers see all records but interleaving across clusters is nondeterministic. Per-region strict monotonic offsets; global order undefined; consumers requiring global ordering are a corner case we do not support in v1.
- **Queues**: same frame machinery mirroring `__queue_attempts`. Delivery converges to: each cluster delivers locally; attempts/acks both sides replicate; whichever side records the final acknowledge or max-attempts first wins; the lagging side converges by observing mirrored state (acked → ack locally without redelivery; budget exhausted → divert to DLQ locally). The convergence window equals peer replication lag. Exactly-once across the federation is not provided, only "at-least-once per cluster, at-most-once-eventual across both" (requires the lag to be zero at the race instant — unattainable distributed-guaranteed; documented).
- **Split-lane hazard**: two clusters that *both* seal blocks for the same topic simultaneously produce two independent, equally valid byte-identically blocks for the same content in different physical locations; the content-hash model still lets every copy federate safely. No conflict: federation never depends on relative ordering, only on the id → content binding.

## Cursor semantics

`__federation_cursors` (internal, cluster-local, compacted):

```
key:    {peer, topic, partition, source_offset_base}
value:  {last_source_offset, last_local_offset, mode, applied_unix}
```

- `__federation_peers` and `__federation_cursors` are **never** federated (cluster-local by construction; explicit blocklist of internal names in the puller's requested topics, enforced in code).
- The log is the source of truth; `FetchResponse.federation_hwm = min(cursor.last_local_offset)` across all peers configured for the topic — a conservative "all peers caught up" signal for the client. 0 means unknown / not federated. The annotation is advisory and must not be trusted for correctness decisions made across clusters.
- Cursor log compaction runs by upsert key equality; reverting a peer to zero (full re-pull) requires operator action: `DELETE /federation/peers/{name}` + re-register. Nothing silently re-imports everything after an operator wipe — data integrity over convenience.

## Security posture

- Federation keys are bearer tokens with `federation` role, stored SHA-256-hashed like all keys (ADR-008).
- Federation RPCs require the federation role; normal produce/fetch/ack keys never satisfy them.
- TLS on the federation channel is mandatory — policy validation requires `ca_cert` when peer `tls` is true; a peer with `tls: false` is allowed only with an explicit `allow_plaintext: true` CLI flag on the registering side (self-certified), audited.
- Registration is out-of-band; registries never auto-discover each other.
- Federation records are audited (`__audit`) on every mutation and refused peer contact.

## Failure modes (explicit)

| Failure | Behavior | Recovery |
|---|---|---|
| Peer unreachable | puller warns + exponential backoff; cursor stalls; metric `firn_federation_cursor_lag_seconds` rises; **no data loss** | auto-resumes on reconnect |
| Partition count mismatch | registration rejected with error naming the counts | operator aligns configs |
| Content mismatch persists | frame parse/CRC error; range dropped; cursor untouched; cluster log identifies the offender | operator investigates; no silent skip |
| Local wipe / empty restart | full re-pull from cursor=0; duplicates land as new records (document: treat as disaster re-seed, not cleanup) | operator resets peer if full re-import unwanted |
| Conflicting mode policies | only consistent registrations federate; both sides must agree on direction or no data moves | operator fixes side(s) |
| Peer floods frames | each imported frame is CRC-verified and offset-checked by the normal append path; no new attack surface; backpressure by batch/interval flags | rate limit via `-federation-interval` / `-federation-batch` |

## Consequences

- A second replication engine exists (inter-cluster) with different properties than ADR-007 (cluster-local): accepted; the engines share no code paths beyond the frame codec and do not share trust domains.
- Cross-cluster ordering is undefined for active-active topic writes; consumers must not depend on it.
- Queue federation is at-least-once per cluster, at-most-eventual once both clusters have seen each other's state.
- `__federation_peers` and `__federation_cursors` are new internal topics (single-partition, cluster-local, compacted).
- Clients learn one new field (`federation_hwm`) and one new admin block; old clients see the hint as zero and are unaffected.
- Operator ergonomics: registration is two explicit operator steps, documented in install/config/api.
- Federation key material is forever out of band; no single `firn federation join <code>` — that is deliberately not v1 (no auto-discovery, no pairing protocol to attack).

## Non-goals (this ADR)

- Push federation (inbound from peers) in v1.
- Cross-cluster consumer-group offset sync (`__consumer_offsets` is cluster-local by design; each side groups independently).
- Global message ordering across clusters.
- Exactly-once queue consumption across clusters, ever.
- Scheme/schema registry federation.
- Conflict resolution for user metadata that inadvertently diverges (topics created on both sides with the same name but different modes). Policy requires explicit reconciliation: one side wins by operator decision.

## When this plan changes

Scope creep of federation beyond this ADR (push mode, cross-cluster group-sync, exactly-once) requires a new ADR and never a silent expansion of a release.
