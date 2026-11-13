# Changelog

## v0.1.0 — 2026-11-13

First MVP release against the ADR-005 definition of "working". Single-node; no replication, exactly-once, wire protocols, or auth (ADR-001 non-goals).

### Added

- **Streaming mode**: topics with N partitions; FNV-1a key-based partition assignment (round-robin for keyless); fetch by offset; replay. HTTP admin: `POST /topics`, `GET /topics`; gRPC `Produce`/`Fetch`.
- **Durable log** (slice 2): segmented files per partition (`<data-dir>/<topic>-<partition>/<baseOffset>.log`), CRC-validated length-prefixed frames, sparse in-memory index rebuilt on open, fsync per append and on segment roll, torn-tail truncation on open; `kill -9` fault-injection test in CI.
- **Topics & retention** (slice 3): topic metadata persists to `topics.json` (atomic tmp+rename+fsync); retention deletes whole segments only, opt-in via `-retention-ttl` / `-retention-max-bytes` (swept every `-retention-interval`); offsets below the retention horizon return `FailedPrecondition`.
- **Consumer groups** (slice 4): `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` over gRPC; range + roundrobin assignment; member lease expiry (10s default, `-session-timeout`); committed offsets in the internal compacted `__consumer_offsets` topic (survive restart; membership does not); HTTP `GET /groups`, `GET /groups/{name}/offsets`; committed offsets advance monotonically unless `allow_reset`.
- **Queue mode** (slice 5): topics with `mode: "queue"`; `Pull`/`Ack` over gRPC and HTTP (`POST /queues/{q}/pull|ack`); visibility lease (30s default); redelivery after lease expiry; `__queue_attempts` internal store; after 3 delivery attempts messages divert to `<queue>.dlq` (auto-created). At-least-once, per-message.
- **Operability** (slice 6): `/metrics` (Prometheus: `firn_produce_total`, `firn_fetch_total`, `firn_fsync_seconds`, `firn_group_rebalances_total`, `firn_queue_depth`); JSON structured logging via `log/slog` (HTTP + gRPC request logs); `firn-cli` (`create-topic|create-queue|list|produce|fetch|groups|dlq list|redrive`); benchmarks in `bench/` (baseline: `bench/baseline.md`); CI runs `golangci-lint` v2.15.0.

## v0.2.0 — 2026-11-13 (ADR-007 scaled deployment, preview behind `-node-id`/`-peers`)

Phases A–F and H delivered against `docs/plans/002-ha.md` (phase G, S3 archive tier, explicitly deferred):

- **Phase A — block engine**: `FileLog` derives content-addressed sealed blocks (`SHA-256` of the frame bytes), early time-based sealing (`WithBlockSealing`), immutable blocks feeding descriptor-based replication.
- **Phase B — embedded metadata quorum**: in-house Raft (pre-vote, no new deps) owning a metadata log: topic-create entries fan out to every node's local registry via the quorum apply hook; block commits and lease grants ride the same log. Replay on restart exposes only committed entries.
- **Phase C — replication & failover**: block push/pull replication; partition write leases (epoch-fenced) with TTL; unsealed tails re-replicate after lease hand-over; byte-identical replicas verified. Fault-injection: kill holder → survivor takes lease → zombie writes rejected.
- **Phase D — client routing**: `GetMetadata` RPC returns per-partition lease holder addresses; `firn-cli` routes produce/fetch to the holder with refresh-on-retry.
- **Live 3-process smoke test** (TCP quorum, SIGKILL of a node, rejoin, pull catch-up): verified.

**Known gaps of this preview** (move before anything ships as default):

1. Lease re-grant on expiry lands on the current quorum leader (not a re-balanced owner) — expect leader affinity after failovers.
2. Unsealed tail (≤ seal interval) is the at-risk window on node death.
3. Static membership — no dynamic join/leave; reviving a cluster requires the original ids.
4. Not reviewed or committed — nothing from ADR-007 is merged; this preview exists to validate the design end-to-end.

### Known limitations / explicitly deferred

- fsync-per-append on the produce path: ~500 msg/s per broker on a developer machine (Apple M3 Pro). **Decision (2026-11-13, ADR-005 amendment): fsync-per-append is kept; the ≥ 50k msg/s produce bar is retired.** The flush-window policy remains a possible future storage-semantics change — its own ADR, property tests, mandatory storage review.
- `__consumer_offsets` and `__queue_attempts` are physically compacted only at clean shutdown and every 1000 commits; unbounded growth between compactions is bounded by commit rates, not by deletes.
- `git tag v0.1.0` and binaries are cut by a human from a clean checkout — see the release checklist below; the agent left them undone deliberately.

### ADR-005 acceptance checklist — run 2026-11-13 on macOS/arm64 (M3 Pro)

| # | Item | Result |
|---|---|---|
| 1 | `firnd` starts, `/healthz` ok, topic created via HTTP admin | ✅ manual run |
| 2 | gRPC produce: 100k messages to a 3-partition topic, no corruption | ✅ verified live (byte-level payload check on read-back) |
| 3 | gRPC fetch by offset returns ordered, byte-identical payloads across restart | ✅ verified live (kill -9 → restart → payload compare) |
| 4 | `kill -9` during produce; no torn record served; no acknowledged message lost | ✅ `TestFileLogSurvivesKill9` (SIGKILL child, reopen, assert) + manual run |
| 5 | Consumer group `g1`: two members, rebalanced, committed offsets resume after restart | ✅ `test/e2e_test.go` (600 msgs, 3 partitions, restart, resume) + live check |
| 6 | Queue mode: unacked redelivered after timeout; DLQ after 3 attempts | ✅ `internal/queue` tests (redelivery, poison → DLQ) + live run |
| 7 | `go test ./...` incl. fault-injection recovery tests passes in CI | ✅ locally `go test ./... -race -count=1` green; CI runs vet + test + build + golangci-lint v2.15.0 |

Manual-run caveats: (1) a clean-VM run was not available to the agent; all checks above ran on the development machine. (2) The repo was not committed by the agent; tagging `v0.1.0` requires a human to review the uncommitted diff, commit, and tag.
- **Phase E — security & admin UI**: bearer auth on both surfaces (required once any API key exists), roles `viewer`/`operator`/`admin`, per-key token-bucket rate limit, TLS on both listeners + CLI `-ca-cert`, `__api_keys` + `__audit` internal topics, read-only admin UI at `/ui/`, CLI `create-apikey|revoke-apikey|list-apikeys`.
- **Phase F — internal topics replicated**: `__consumer_offsets`, `__queue_attempts`, `__audit`, `__api_keys` are broker-owned shared `FileLog`s replicated like ordinary user topics (block push/pull, lease-fenced). Committed group offsets are visible on every node via read-through stores.
- **Docs (ADR-008)**: `docs/install.md`, `docs/configuration.md`, `docs/api.md` — install guide, full flag reference, complete HTTP+gRPC API reference with live-tested examples and error tables. README links them; retention rules and storage validation written down in ADR-009.
- **Retention tunable on the daemon**: new `-segment-bytes` (default = broker 64 MiB) and `-block-seal-interval` (roll active segment on age, even without size pressure) flags, plumbed to `broker.WithMaxSegmentBytes`/`WithBlockSealing`. Retention verified end-to-end live: segment roll → sweep (`deleted_segments`) → sub-horizon fetch returns `FailedPrecondition`.
- **Data race fix (found by `-race` after Phase F)**: `FileLog` is now serialized by an internal leaf mutex — the broker, group coordinator, and replication agent share the internal topic logs concurrently and previously raced in `SealIfOlderThan` vs. `Append`. Package contract changed from "callers serialize access" to "all exported methods are goroutine-safe" (fsync holds the lock by design).
- **ADR-005 amended**: keep fsync-per-append; the ≥ 50k msg/s produce bar is retired. Warm-segment fetch bar (p99 < 20 ms) retained. ~500 msg/s produce per node is the documented expectation; a flush-window ADR would be required to change it.
- **Phase F completion — single-writer fencing**: internal-topic writes (`__consumer_offsets` commits, `__queue_attempts`, `__audit`, `__api_keys`) are fenced by the internal lease on a shared `fence.GateBox`; non-holders fail closed (HTTP 503 / gRPC `FailedPrecondition`) with the holder in the message. Queue `Pull` errors instead of delivering nothing. CLI `create-apikey`/`revoke-apikey` retry across seed nodes on 503. `TestInternalTopicSingleWriter`: concurrent commits yield exactly one writer; replicas converge byte-identical.
- **Read path fix (internal stores)**: `OffsetStore`, `AttemptStore`, and the apikeys store now do **HWM-gated read-through** (O(1) fold reuse while high watermark is unchanged; full re-fold only on movement). Cross-node commits/apply are visible without restart — and a failed-over holder no longer serves a stale startup-folded view.
- **Compaction refused on replicated internal logs**: `FileLog.Compact()` renumbers records, which would silently diverge byte-identical replicas; fenced stores refuse `Compact()` and skip auto-compact. Single-node compaction unchanged. `__consumer_offsets`/`__queue_attempts` growth in cluster mode is now bounded by commit rate, not by compaction — capacity planning note for the release checklist.
- **`-race` fix in `FileLog`** (see Data race fix) — also required the package contract change.

## Unreleased

- ADR-010 plan 004 slice 4 delivered: **active-active federation (preview)**. Both clusters accept writes to the same topic; records re-origin with fresh local offsets (ts/key/value preserved); the A→B→A echo is filtered out at the serving side via each node's foreign-applied local ranges (`-federation-self` + `requesting_peer` in `FederationFetch`; `FederationReport` now returns `partitions` for pull-time registration validation). `federation_hwm` now covers all inbound peers. Live two-cluster verification: converged both ways, zero duplicates over repeated polls, fresh-sink full import, partition-mismatch rejection, crash-resumption. Passive mode unchanged.
- ADR-010 plan 004 slice 3 delivered: **client-visible federation progress**. `FetchResponse.federation_hwm` (advisory minimum across passive peers covering the partition; 0 until first import; never a correctness input). `firn_federation_cursor_lag_seconds{peer,topic,partition}` metric — 0 while caught up, rising while a peer is unreachable (puller-tracked; verified by unit test). Read-only admin UI at `/ui/federation` (peers + import cursors). `firn-cli fetch --json` now reports `federation_hwm`. `firn federation status` was already shipped in slice 1.
- ADR-010 plan 004 slice 2 delivered: **passive one-way federation (preview)**. `FederationFetch` serves raw CRC-checked frame windows from the source over the mesh listener (mTLS + federation key). The sink puller (`-federation-interval 500ms`, `-federation-batch 1024`, `-federation-max-range 1 MiB`) imports byte-identically — offsets/timestamps/keys/values preserved, segment files `cmp`-identical in live two-cluster verification; crash-resume via persisted `__federation_cursors`. Sink is fenced: produce → gRPC `FailedPrecondition`, HTTP `409`; queue pull/ack → HTTP `409`; federation keys can fetch and nothing else. `__queue_attempts` mirrored byte-identically all-or-nothing; queue message logs ride along with named queues; `firn-cli federation add-peer` accepts `--key`/`--server-name`. Active-active remains for slice 4; cutover runbook in slice 6.
- ADR-010 plan 004 slice 1 delivered: secure **federation control plane** (no data motion). New internal replicated logs `__federation_peers` / `__federation_cursors`; new `FederationService` (returns Unimplemented — pull/push paths land in later slices); new `federation` key role usable on mesh listener only; admin endpoints `POST/GET/DELETE /federation/peers`, `POST /federation/keys`, `GET /federation/cursors`; CLI `firn-cli federation add-peer|list-peers|remove-peer|keys|status`. Persistence across restart verified; federation-scoped keys cannot call non-federation RPCs; strict-boot mode refuses unauthenticated federation reads.
- ADR-011 slice 6 delivered: dist job on `main` runs `go build -trimpath -ldflags "-s -w"`; SPDX SBOM via anchore/sbom-action; grype fail-on-critical gate via anchore/scan-action. Cosign sign-blob documented as manual release step (see docs/distribution.md). Docs: `docs/migration-v0.3-to-v0.4.md` upgrade guide, `docs/distribution.md` operator runbook, README status bumped to v0.4.0-preview, install.md leads with secure-by-default callout, configuration.md tabulates new flags (`-dev`, `-allow-open-bootstrap`, `-metrics-auth`, `-anon-rate`, `-anon-window`, `-dir-mode-check`, `-audit-max-bytes`).
- ADR-011 slice 5 delivered: internal topics `__api_keys-0`, `__audit-0`, `__consumer_offsets-0`, `__queue_attempts-0` now created with mode `0700` and files `0600`; `-dir-mode-check` (default true) refuses to start if any file under `-data-dir` is permissive (override with `-dir-mode-check=false` for lab/dev or exotic filesystems); `-audit-max-bytes` (default 64 MiB) bounds the replicated `__audit` topic per process and refuses writes with HTTP 503 past the budget (in-memory MemStore wired for `-data-dir=""`; reads unaffected). Automatic audit rotation is operator-driven (documented), not automatic.

- ADR-011 slice 4 delivered: anonymous traffic capped per source IP (`-anon-rate`, default 20 ops per minute) on /healthz and all unauthenticated refusals; API keys may carry an `allowed_ips` CIDR allowlist with mismatched callers receiving HTTP 403 / gRPC PERMISSION_DENIED (`firn-cli create-apikey --allowed-ips 203.0.113.0/24`); `authz-refused:ip-not-allowed` is audited; audit logging in every mode (not just enforced).
- ADR-011 slice 2 delivered: **mesh mTLS**. `ReplicaService`/`QuorumService` served only on a dedicated mesh listener behind mTLS — invisible to the public gRPC port. Cluster CA pre-provisioned once via `firn mesh-ca init` (or `-mesh-ca-bundle`/`-mesh-ca-key`) and copied per node; node certificates issued at first start. Untrusted clients cannot complete the handshake; streaming/replication verified live on 3 nodes (produce on holder, fetch on replicas, kill -9 survives).
  - **Note**: mixing old/new mesh builds on a live cluster is not supported; rolling upgrade requires a full restart window.

- ADR-011 slice 1 delivered: secure-by-default startup + one-time bootstrap token. Zero-key broker prints a single-use token on stderr; `POST /apikeys` with that token mints exactly one admin key (role coerced to admin), then bearer auth enforces everywhere except `/healthz`. `-dev`/`-allow-open-bootstrap` re-enables open mode for local work. `/metrics` now requires a key by default. Verified live (fresh boot → mint → produce/fetch → restart enforced). See `docs/plans/005-security.md`.

- ADR-011 secure-by-default + Plan 005 added: fresh installs without a key no longer listen (fail-closed; one-time bootstrap token on stderr; `-allow-open-bootstrap`/`-dev` opt-out); mesh transport becomes mTLS-mandatory (closes F-2/F-3); `/metrics` now requires a key by default; per-key CIDR allowlist; global anonymous rate limit; data-dir `0700`/files `0600` enforced; audit always-on, bounded, append-only; HTTP hardening headers; release hardening (SPDX SBOM, cosign-signed tarballs, grype critical gate). Implementation via plan 005; storage-adjacent review gates on slices 2 & 5.
- ADR-010 federation + plan 004 added (multi-writer content-hash merge; passive one-way DR; client-visible federation cursors via `__federation_cursors` + `FetchResponse.federation_hwm`; API-key-registered peers; pull-only v1; queues federate fully with attempt-state mirroring).
- **BREAKING**: module path, binaries, proto package, metrics prefix, env vars renamed to Firn (see separate commit); MIT license added; repo published as rahulkj/firn public with MIT.

## v0.3.0 — 2026-11-13 (BREAKING) — Firn is the durable event broker

**Eventing is now Firn** (module rename, no functional change). The rename covers every user-facing identifier; the next release is breaking by design. Note the naming history, kept honest: the project was first renamed to **Slipway**, then rejected after conflict sweeps found four live software companies under that name (incl. a funded SF AI-onboarding startup in the same space); **Firn** (compacted snow layers — the compaction/retention metaphor) swept clean in two independent searches.

MIT **License added** (2026-11-13).

- Module path `github.com/rahulkj/eventing` → `github.com/rahulkj/firn`
- Binaries `eventingd` → `firnd`, `eventing-cli` → `firn-cli`; env vars `EVENTING_*` → `FIRN_*` (e.g. `FIRN_ADDR`)
- Proto package `eventing.v1` → `firn.v1`, service `FirnService`, Go package `firnpb`; stubs regenerated with buf
- Metrics prefix `eventing_*` → `firn_*`
- Zero Kafka/RabbitMQ references anywhere in the tree; README/AGENTS/ADRs rewritten to describe the broker on its own terms
- Naming: all other candidates from the shortlist are conflicted (Strata→jsedlak/strata; Outbox→pattern keyword; Palimpsest→palimpsest-wal; Wick→Wick streams framework; Gantry/Talus/Ember/Basin/Waymark/Fermata→all live software companies in adjacent spaces). See docs/license-audit.md for the full table
- `docs/license-audit.md`: engineering due-diligence showing no code from either incumbent is incorporated (dependency graph + marker scans). **Not legal advice.** LICENSE landed (MIT); formal trademark clearance for "Firn" still required before public promotion; optional rename of the `__consumer_offsets` internal topic to `__group_offsets` to erase the last incumbent-named identifier.
- Quorum liveness fix (see separate commit): per-election re-randomized timeout — equally-timed peers could split votes and never elect a leader after the leader died (observed intermittently under full-suite `-race`)
- version constant bumped to 0.3.0

- Docs consistency pass (post-v0.2.0): ADR status lines aligned with what actually shipped (001–006 delivered v0.1.0; 007 delivered as v0.2.0; 005 amended; 009 validation + gap-fix shipping); ADR-007 lease default corrected to 3s; `docs/api.md` internal services section now documents `ReplicaService` and `QuorumService` with request/response examples (frame layout, Raft envelope, base64-verified examples) as scoped by ADR-008. Documentation ADR (008) is now fully delivered against its own checklist.
