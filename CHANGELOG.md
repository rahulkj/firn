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
