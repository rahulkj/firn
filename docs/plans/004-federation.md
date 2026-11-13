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
| 4 (**delivered 2026-11-13**) | `active-active` full mode: frame pull + re-origin through `broker.AppendReoriginedFrames` (fresh local offsets, ts/key/value preserved); server-side foreign-range echo filter on `FederationFetch` (+ `requesting_peer`, `-federation-self`); registration validation at pull time via `FederationReport.partitions`; cursor records foreign local ranges with coalescing; `federation_hwm` counts all inbound peers | **mandatory (storage-adjacent) — owner review pending** |
| 5 (**delivered 2026-11-13**) | Active-active queues: `__queue_attempts` both-way replication with re-keyed offsets (foreign-range translation); `FederationReport.foreign_ranges` published by the server; convergence test (competing consumers in both clusters → every job consumed exactly once per cluster); DLQ contents converge on both sides | **mandatory — owner review pending** |
| 6 (**delivered 2026-11-13**) | ADR validation pass vs shipped behavior (implementation-notes addendum in ADR-010); install.md federation quickstart; federation-cutover runbook (`docs/federation-cutover.md`); api.md federation surface sync; README federation paragraph; AGENTS.md cross-link | docs review |

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

Delivered 2026-11-13. Deltas vs the original spec (this section is authoritative):

- **Re-origin, not re-encode**: imports apply via `broker.AppendReoriginedFrames` — local offsets are assigned sequentially, ts/key/value are preserved verbatim. No change to `internal/log`; the new broker method mirrors `AppendReplicatedFrames`' locking and file-backed requirement.
- **Echo filter lives at the serving side**: `FederationFetch` now omits frames in the served window that THIS cluster imported from the `requesting_peer` (foreign-applied local ranges, recorded per cursor and coalesced on write). A sends its own `-federation-self` name; the serving side matches it against its registered peer list. There is no bypass: frames A donated to B never appear in B's exports to A. Content-hash dedup was NOT needed and is not used.
- **Requesting peer identity**: the serving filter trusts the `requesting_peer` hint unconditionally. Worst case: a peer claims another origin and is served a subset of its own-donated bytes — a self-DoS consistent with the federation trust model (federation keys are mutual-trust bearer tokens; the protocol is not byzantine-hardened). Both clusters should register each other symmetrically; docs cover the layout.
- **Registration validation is pull-time, not commit-time**: `FederationReport` now returns `partitions`; the puller refuses to import and logs loudly on a mismatch (topic missing on sink → also skip with a hint to pre-create it). Partition-count equality is the only cross-side invariant enforced mechanically; mode conflicts remain operator-enforced (active-active allows writes on both sides by design).
- **Crash semantics**: a crash between re-origin append and cursor commit duplicates at most one batch (at-least-once); exactly-once remains a non-goal (ADR-010). The byte-identical passive path is unchanged and idempotent.
- **`federation_hwm`** now counts ALL inbound peers (passive and active-active) covering the topic/queue — not passive only as shipped in slice 3.
- **Verification**: unit/integration under `-race` (two in-process clusters, bufconn): cross-direction convergence to identical content with distinct local offsets; echo stability (repeated polls with no input duplicate nothing); 25-record fresh-sink import with re-origined offsets; partition-mismatch rejection (zero import, no cursor); live two-cluster smoke with `-federation-self`: both clusters converge to the same record set, `federation_hwm` reports the source HWM both directions, zero duplicates after multiple round-trips.
- **Not in this slice**: active-active `__queue_attempts` both-way replication → slice 5. `__federation_peers/cursors` still local. Frame format unchanged.

### Slice 5 — active-active queues

Delivered 2026-11-13. Deltas vs the original spec (this section is authoritative):

- Attempts mirror both ways with **re-keying**: attempt keys encode the source cluster's message-log offsets, which are independent per cluster. The mirror translates them into local offset space via each node's foreign-applied local ranges (recorded on import, coalesced). When this node never imported the record (its peer authored it), the source's ranges are consulted instead — `FederationReport` now returns `foreign_ranges` (additive proto field) so the inverse mapping is available for either direction.
- The `__queue_attempts` mirror fires whenever a queue-like topic is in scope (a topic registered locally as queue-mode, a queue named directly, or an unrestricted selector). Passive peers keep the byte-identical all-or-nothing mirror; active-active peers re-key in scope and drop out-of-scope keys deliberately.
- Competing consumers on both clusters deliver independently; attempts fold `max`; both clusters divert to their local `<queue>.dlq` independently and converge on identical contents (verified: DLQ key sets equal on both sides).
- Ownership fix landed with this slice: `internal/groups` and `internal/queue` stores no longer close the broker-owned `__consumer_offsets`/`__queue_attempts` shared handles — they borrowed them and ownership was unclear (an untimely `Close()` poisons in-flight replication of the shared log). Stores only close handles they opened themselves.
- Verification: integration under `-race` (both-active-active claims on both sides, attempts converge to identical values, DLQ contents identical); live two-cluster run (produce on A, claim on B, attempts mirrored back to A and translated).
- `__federation_peers` and `__federation_cursors` remain never-federated. Queue message logs in passive mode still ride with named queues (slice 2 semantics, unchanged).

### Slice 6 — docs, ADR validation, release readiness

Delivered 2026-11-13. Notes:

- ADR-010 "consequences" re-verified vs shipped behavior; deviations recorded as an **Implementation notes (2026-11-13)** addendum at the end of the ADR (original consequences preserved verbatim; the addendum wins where they disagree): pull-only confirmed; foreign-range serving-side echo filter; `FederationReport.foreign_ranges` published for either-direction attempts re-keying; re-origin crash semantics (at most one duplicated batch, at-least-once); passive mirror still byte-identical; `federation_hwm` covers all inbound peers.
- `docs/install.md` gains the two-cluster federation quickstart (shared mesh CA → boot with `-federation-self` → bootstrap tokens → federation keys → identical topic on both sides → peer registration both directions → converge-on-write), plus a passive one-way variant with the cutover pointer. Persisted-paths table gained `__federation_peers-0/` and `__federation_cursors-0/`.
- `docs/federation-cutover.md` (new): operator procedure pass→fail for passive → active-active flip or standalone decommission (lag-zero/attempts/partition preconditions, exact commands, verification).
- `docs/api.md` synced: FederationService paragraph, `federation_hwm` advisory note updated (both peer modes), `/ui/federation` link, CLI map.
- README gains the federation preview paragraph and links the quickstart + ADR.
- v0.4.0 remains uncut and untagged (docs-only pass; no release decision taken here).

## Open items (tracked, not blocking)

- ~~Operator cutover runbook~~ delivered in slice 6 (`docs/federation-cutover.md`).
- Metrics polish: `firn_federation_import_{frames,bytes}_total` counters per peer/topic/partition.
- Push-federation shape (ADR-012 candidate): inbound push is strictly better for private networks without outbound access to peers' internal listeners; requires a new ADR and its own plan.
- Mirror retention operator tooling: `firn federation reseed <peer> --topic <t>` (explicit wipe+reimport; never implicit).
