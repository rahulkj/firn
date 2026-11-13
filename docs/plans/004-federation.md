# Plan 004: Federation (ADR-010)

Derived from ADR-010. Proceeds one slice per session, same loop as plans 001–003:

> paste the slice spec → implement → tests first → read the diff → `go vet ./... && go test ./... -race` green → human review for storage-adjacent diffs → commit.

Non-negotiables: storage review for data-plane diffs; plan changes land here first; slice 6 ships with ADR validation + docs, never silently expands scope.

## Slices

| Slice | Delivers | Review gate |
|---|---|---|
| 1 (**delivered 2026-11-13**) | Internal topics `__federation_peers` / `__federation_cursors`; `FederationService` skeleton registered on the mesh listener; registration API-key scope `federation`; admin HTTP endpoints (`POST/GET/DELETE /federation/peers`, `POST /federation/keys`, `GET /federation/cursors`) | API + storage review (shared internal log) |
| 2 (**delivered 2026-11-13**) | `passive` one-way mode: source `FederationFetch` over the mesh (mTLS + federation key); sink write fence on produce/pull/ack (gRPC `FailedPrecondition`, HTTP 409); byte-identical import of topics/partitions via the existing replicated-apply path (offsets/timestamps preserved; `cmp`-identical segments verified live); `__queue_attempts` mirror byte-identical; queue message logs ride along with named queues; sink delivery engine stays idle until cutover | **mandatory (storage-adjacent) — sign-off received 2026-11-13** |
| 3 (**delivered 2026-11-13**) | `FetchResponse.federation_hwm` (advisory, min across passive covering peers; `0` until first import); `firn_federation_cursor_lag_seconds{peer,topic,partition}` gauge (rises while a peer is unreachable; ~0 while caught up); admin UI `/ui/federation` (read-only); `firn-cli fetch --json` reports `federation_hwm` | API + docs review |
| 4 | `active-active` full mode: frame pull + re-origin through existing append path; cursor origin/range tracking; echo prevention by cursor invariant; **no new storage code outside broker/replication/fencereplica layers** | **mandatory (storage-adjacent)** |
| 5 | Active-active queues: `__queue_attempts` both-way replication; convergence test (competing consumers in both clusters → exactly one cluster processes); DLQ convergence test | **mandatory** |
| 6 | ADR validation pass vs ADR-010; install/config/api additions; AGENTS.md cross-link; release note | docs review |

## Slice details

### Slice 1 — registration & control plane (no data motion)

- `internal/federation`: `Peer`, `Policy`, `Cursor` types; `__federation_peers` and `__federation_cursors` internal single-partition logs (cluster-local, compacted on shutdown like `__api_keys`).
- `FederationService` registered on the existing gRPC listener; `FederationFetch` RPC shape in place but returns `Unimplemented`.
- API keys gain `federation` role (`apikeys` schema extension — additive, backward compatible).
- Admin endpoints listed above; register validates policy shape (direction, mode, partition counts) but performs no reachability check yet.
- `firn federation add-peer/list/remove/status` CLI skeleton.
- **Gate: merge after API review only; no data moves.**

### Slice 2 — passive one-way mode

Delivered 2026-11-13. Notes and deltas vs the original spec (this plan section is authoritative):

- Registration now carries the peer-issued bearer `key` (required for direction `in|both` — ADR-010's "addr + CA + key + policy"; `out` is admitted as policy but moves no data in v1, since federation is pull-only); key is never echoed by any endpoint (`keySet` is reported instead). Optional `server_name` for peers whose cert SANs don't match the dial host. Selectors reject `__`-prefixed internal names at registration; the shared CA is the assumed peer-verification topology (a peer-pinned CA may be supplied via `ca_crt_pem`).
- `__queue_attempts` is mirrored **byte-identically, all-or-nothing** when any queue is in scope; a filtered per-queue attempts mirror is impossible in a byte-identical world (offset forks). Attempts carry no payload. A sink with a non-empty attempts log that has served a federated queue must be reseeded (documented in ADR-010).
- Named queues in a selector import the queue's **message log** too (queue-mode topic), not only the attempts bookkeeper; byte-identical as well.
- Selector semantics matching ADR-010: only a completely empty selector expands to "all local topics"; `queues: ["jobs"]` alone never pulls unrelated streams.
- Live two-cluster verification (shared mesh CA, strict bootstrap): byte-identical `cmp` of segment files on both sides for topics, queues, and `__queue_attempts`; crash-resume via persisted cursors; produce on sink → `FailedPrecondition`/HTTP 409; queue pull/ack on sink → HTTP 409; federation keys can `FederationFetch` and nothing else; federation keys cannot produce.
- Federation puller nodes hold no output credentials; the sink authenticates **to** the peer. No inbound port exposure is added on either side.
- Out of scope (documented): per-queue attempts filtering; push federation; metrics (`firn_federation_*`) land in slice 3.

### Slice 3 — cursor surfacing

- Persist cursor upserts to `__federation_cursors` at import-time (not at peer-add time): `{peer, topic, partition, last_source_offset, last_local_offset, mode, applied_unix}` (single upsert contract used by slices 2, 4).
- `FetchResponse.federation_hwm` = `min` across peers configured for that topic — 0 until first import.
- `firn_federation_cursor_lag_seconds` gauge (per peer/topic/partition).
- `firn federation status` — human + `--json`.
- Admin UI `/ui/federation` — read-only table: peer, mode, topics, cursor/lag per partition, last import time.

### Slice 4 — active-active topics

- Puller switches to frame-level import for `active-active` topics: request `[last_source_offset, source_hwm]` frame bytes; parse with the existing CRC-checked reader; **re-encode each frame with the next local offset** (offset field changes; key/value/ts preserved); append to local log; upsert cursor `{origin: remote}`.
- Cursor tracks foreign-applied ranges; puller's completeness filter **skips frames whose source range is already foreign-applied-by-us**, which prevents re-importing a frame that bounced A→B→A (no headers, no origin IDs — the cursor is the filter, per ADR-010).
- Registration validation: partition counts must match on both sides; mode conflicts are a hard error at commit time.
- Tests (all -race): convergence — produce on A & B concurrently → both clusters receive both sets of records byte-exact (re-origined); echo test — A event must never land in A via A→B→A path; partition mismatch rejection; puller resumption from cursor; full-topic import from empty state.

### Slice 5 — active-active queues

- `__queue_attempts` becomes the fifth federated-by-default internal topic when a queue is in scope (the in-federation apply path applies the frame re-encode rules equally).
- Attempts/acks converge naturally: each side delivers independently; attempts mirror both ways; DLQ divert at budget exhaustion is the converged terminal state.
- Property tests: two-cluster competing-delivery simulation (bounded consumers on both sides) → every message consumed exactly once *per cluster* (or DLQ'd in the starved cluster); attempt counts match within lag; DLQ contents converge.

### Slice 6 — docs, ADR validation, release readiness

- ADR-010 "consequences" re-verified against shipped behavior; update if shipped semantics deviate (in the same commit).
- `docs/configuration.md` gains federation flags & `__federation_peers/cursors` note; `docs/install.md` gains a cluster-federation quickstart; `docs/api.md` gains `FederationService` reference; README federation paragraph.
- Release notes: federation is **preview**; two-cluster requirement; push mode unavailable.

## Open items (tracked, not blocking)

- Operator cutover runbook for passive→active flip (write follow-up doc in slice 6).
- Metrics polish: `firn_federation_import_{frames,bytes}_total` counters per peer/topic/partition.
- Push-federation shape (ADR-012 candidate): inbound push is strictly better for private networks without outbound access to peers' internal listeners; requires a new ADR and its own plan.
- Mirror retention operator tooling: `firn federation reseed <peer> --topic <t>` (explicit wipe+reimport; never implicit).
