# ADR-009: Storage solutions — validation and roadmap

Status: accepted · 2026-11-13 · delivered (validation published; follow-ups tracked) · gap fixes (segment-bytes/block-seal-interval flags) shipped 2026-11-13

## Context

Operators asked the only fair question of a storage engine: what does it actually support today, and what happens to bytes when things go wrong? Three storage classes were put on the table: local disk, in-memory, and object storage (S3) as a sync/archive tier. This ADR validates the current implementation against each class and records the honest gaps.

## Validation matrix

Legend: ✅ implemented & verified · 🟡 implemented but partial/unsafe at scale · ❌ not implemented

### 1. In-memory

| Capability | Status | Evidence |
|---|---|---|
| Produce/fetch, groups, queues, all APIs | ✅ | v1 slice-1 design + full test suite (`internal/…`, `-race`); `-data-dir=""` selects `MemLog` for every partition log |
| Durability across restart | ❌ by design | Documented in README "Status" and slice-1 plan; single-process crash loses everything |
| Replication/durability of internal topics | ❌ (in-memory mode stays in-memory) | `-data-dir=""` runs `__consumer_offsets`, `__queue_attempts`, `__audit`, `__api_keys` as `MemLog` too — nothing replicates |

### 2. Local disk (file-backed, default)

| Capability | Status | Evidence |
|---|---|---|
| Segmented append-only log per partition, CRC frames | ✅ | `internal/log/filelog.go`; `filelog_test.go`, `retention_test.go`, `compact_test.go`, `kill_test.go` |
| Crash recovery: torn-tail truncation, CRC validation, index rebuild | ✅ | `TestFileLogSurvivesKill9` (SIGKILL re-exec child) + `TestFileLogRecoveryTruncatesTornTail/CRCCorruption`; verified live (kill -9 → restart → re-read identical bytes) |
| Topic metadata on disk (`topics.json`, atomic rename, fsync) | ✅ | `internal/meta/meta.go`; `meta_persist_test.go`; verified across kill -9 restart |
| `__consumer_offsets` + `__queue_attempts` internal topics on disk | ✅ | The same `FileLog` via `internal/groups/offset_store.go` and `internal/queue/attempts.go`; broker-owned shared handles since phase F; replicated block-byte-identical across cluster nodes; commit/fetch/DLQ round-trip verified live |
| `__api_keys` + `__audit` internal topics | ✅ (phase E) | Same broker-owned shared FileLog model; SHA-256-hashed keys, plaintext shown once; every admin mutation + authz refusal audited; replicated since phase F |
| Retention: delete whole segments by TTL/bytes | ✅ (2026-11-13) | `FileLog.RetainOlderThan`/`RetainOverBytes` + `broker.Retain` + daemon sweeper (`-retention-ttl`/`-retention-max-bytes`/`-retention-interval`). Unit-tested (`retention_test.go`) **and verified live**: `firnd -segment-bytes 2000 -retention-max-bytes 1200 -retention-interval 500ms`, produced records, sweeper deleted a whole 2 KB segment (`deleted_segments=1`), `fetch --offset 0` returned `FailedPrecondition: offset below retention horizon`. `-segment-bytes` and `-block-seal-interval` are now daemon flags. |
| Compaction of internal topics (`__consumer_offsets`, `__queue_attempts`) | ✅ | `FileLog.Compact()` folds last-value-per-key; runs every 1000 commits and on clean shutdown; `compact_test.go`; verified live (`dlq list`/`redrive` sequence) |
| Cluster replication of sealed blocks (ADR-007) | ✅ (phases A–C) | Byte-identical replicas, kill -9 → lease failover → pull catch-up verified live; quorum (in-house Raft) persists `hardstate.json` + `log.bin` under `metadata/` |
| Cluster replication of internal topics | ✅ (phase F, 2026-11-13) | `TestInternalTopicReplicates`: block committed on node A visible on B and C; committed offsets read-through visible everywhere; all four internal topics ride the same block push/pull path as user topics |
| Data-directory portability (copy `./data`, restart) | ✅ (copy within same segment layout) | Untested for cross-version layout; layout is internal and un versioned — document as unstable until a format version is stamped |
| File permissions / encryption at rest | ❌ | Plain files; OS-level permissions only; no encryption driver |

### 3. Object storage as archive/tier (S3 et al.)

| Capability | Status | Evidence |
|---|---|---|
| Pluggable storage backend interface | ❌ | `internal/log` exposes concrete `MemLog`/`FileLog`; no seam for backends; broker depends on concrete logs via unexported `partitionLog` interface |
| Block-blob archive (ADR-007 block model) | 🟡 | Blocks are content-addressed and immutable — the right shape for archive. **Not implemented**: no S3 client, no `__tiered` topic, no push/pull. Phase G of plan 002-ha holds this. |
| External sync (backup/restore tool) | ❌ | None exists; nothing in `cmd/` touches an object store |
| Cross-cluster replication | ❌ | Out of scope for v1 (ADR-001) and unshaped for G of plan 002 until block engine (phase A) + replication (phase C) merge |

## Decision summary

1. **Local disk is the durable path for the foreseeable release.** In-memory exists only for dev/tests/broker-scale benchmarks and must never be the production durability answer (documented in README).
2. **S3/object-storage support is explicitly not shipped.** It is tracked as **Phase G of plan 002-ha**, unblocked by phases A–C of that plan; implementation needs its own plan, an S3 ADR, and an object-store dependency (AWS SDK or lighter signalling — decision deferred). No code, no flags, no stubs.
3. **Retention is real and operator-tunable** (`-segment-bytes`, `-block-seal-interval`, `-retention-ttl`, `-retention-max-bytes`, verified end-to-end). It deletes whole segments only — offsets are never renumbered and the active segment is never deleted; sub-horizon fetches fail with `FailedPrecondition`.
4. **Data-directory format is not versioned.** Until a format version byte exists in `topics.json` and segment frames, upgrades that change layout are unsupported and offsite copies must be treated as opaque.
5. **Encryption at rest and at the wire** are outside v1. Anything beyond OS filesystem permissions requires a separate ADR with real threat model work.

## Required follow-ups (each with its own plan + review gate)

| Follow-up | Where | Blocking |
|---|---|---|
| Expose `-segment-bytes` and `-block-seal-interval` on `firnd` | **done 2026-11-13** (broker flags plumbed; verified live: segment roll, sweep deletion, horizon error) | — |
| Storage format version byte in `topics.json` + segment index | new slice in plan 002 after D | any backup/restore tooling |
| S3 archive tier (Phase G) | plan 002-ha; needs object-store ADR | nothing else |
| Encryption at rest/wire | new ADR (security) | any production deployment |
| External sync CLI (`firn archive export/import`) | new plan | object store |

## Consequences

- Operators get a truthful map of what holds their bytes on disk and what cannot.
- S3 stays a roadmap item with a known shape (block archive behind the broker) rather than a vague promise.
- Retention is presented honestly: correct, tested, but not yet tunable on the daemon.

## Implementation notes (2026-11-13): shipped divergences

Consequences and the S3 section contradicted reality in three ways; shipped divergences recorded:
- "Retention ... not yet tunable on the daemon" (Consequences) is stale since the same-day gap fix — `-segment-bytes`, `-block-seal-interval`, `-retention-ttl/max-bytes/interval` are all shipped and verified live.
- "Cross-cluster replication — ❌ Out of scope for v1" is stale: ADR-010 delivered federation (passive byte-identical + active-active re-origin + queue attempts mirror, preview). Single-cluster replication was delivered as ADR-007 phase C/F.
- "Encryption at rest and at the wire are outside v1" is half-shipped: wire TLS is not out of v1 anymore — TLS 1.2+ on both listeners (`-tls-cert`/`-tls-key`) and mandatory mesh mTLS (ADR-011) both ship; encryption at rest remains OS/FS-level only (`-dir-mode-check` adds hygiene, no KMS).
- Audit rotation shipped **operator-driven**, not automatic (`-audit-max-bytes` refuses writes past budget; rotation is a manual step; see ADR-011 implementation notes).
- `install.md`'s mention of "`.index` sidecar" was inaccurate — indexes are in-memory, rebuilt on open; no persisted index exists.
