# ADR-013: Unbuilt features register — what's missing, what we're doing about it

Status: accepted · 2026-11-13 · living register — checkboxes flip as work lands

## Context

During the v0.4.0 documentation and audit pass, every ADR was cross-checked against the code and this repo. A handful of documented-or-implied-but-never-built features surfaced repeatedly across the ADRs. This ADR is the single honest register of everything that is **not built**, why, and what happens to it next. Nothing here is aspirational marketing; if it's listed, it does not work today.

## What is and is not built

### Client SDKs
ADR-001, ADR-002, ADR-007, ADR-008 all referenced a "Go SDK" (then Python). None shipped. `firn-cli` was used as the reference client for docs and smoke tests.

- [x] **Go SDK** — this is the one the docs referenced most; now shipping under `sdk/firngo/` (see implementation notes below).
- [ ] Python SDK — `pip install firn` is **not real**; use `grpcio-tools` against this repo's `firn.proto` (documented in `docs/developer.md`). Build deferred until someone needs it.
- [ ] Java SDK — Maven/Gradle artifacts are **not real**; build from the proto (documented in `docs/developer.md`).
- [ ] .NET SDK — same; not executed in this environment.

### Storage / reliability

- [ ] **Storage format version byte** in `topics.json` + segment frames — unversioned layout blocks backup/restore tooling and cross-minor-cycle upgrades (`docs/adr/009-storage-solutions.md`, follow-up row 2). Implementing touches storage invariants; will be a dedicated storage review, not a drive-by.
- [ ] **Object storage / S3 archive tier ("Phase G", plan 002-ha)** — blocks are content-addressed and immutable — designed for it, never built. No S3 client, no FUSE, no tier code, no stub flags. Requires its own ADR + human approval for any dependency. Deferred indefinitely until requested by an operator with real numbers.
- [ ] **Persisted sparse index (`.index` sidecar)** — `internal/log` rebuilds the index on open by scanning; persisted index is a performance optimization only, never correctness-critical. `docs/install.md` is accurate in saying there is no sidecar today.

### Tooling

- [ ] **Protocol gateway** (foreign broker protocol ↔ firn) — ADR-002 decision record sketch; no foreign protocols implemented, no proxy exists. Would ship as a separate process, never in the broker. Deferred.
- [ ] **External sync / archive CLI (`firn archive export`)** — backup/restore of a topic and its offsets. Discussed in ADR-009; not built. Export path is simple enough to ship; import with offset re-assignment is the hard half. In scope below (approximate plan; not landed yet).

### Security / compliance

- [ ] **Encryption at rest** (KMS or passphrase) — files are `0600`/`0700`; OS permissions only. Needs its own threat model (KMS vs local passphrase vs HSM), performance on the fsync path, federation key distribution — its own ADR.
- [ ] **Multi-tenancy / per-topic ACLs** — ADR-011 non-goal. Would land as `topic:read|write|admin` scopes per key, not per cluster. Not scheduled.
- [x] **Browser-native auth for `/ui/`** — **delivered 2026-11-13** (plan 005 slice 7). `POST /ui/session` exchanges a bearer (or existing session) for an HttpOnly, SameSite=Strict `Path=/ui/` cookie (12h TTL, `Secure` on TLS); `/ui/*` accepts bearer or cookie; `DELETE /ui/session` revokes; revoke-on-key-revoke. Proxy-with-pinned-viewer-key remains a valid alternative for multi-user deployments.
- [ ] Automatic audit log rotation — **rejected by design** (ADR-011 D-007 shipped operator-driven rotation); recorded here only so the register is complete.

### Non-goals confirmed (never going to happen)

- Wire compatibility with Kafka or any other broker (ADR-001, ADR-002).
- Exactly-once / transactions.
- Cross-cluster consumer-group offset sync.
- Push federation (inbound).
- 50k msg/s on fsync-per-append storage (ADR-005 amended — bar retired honestly).

## Plan (priority order)

1. **Slice A — Go SDK** (`sdk/firngo`): connect (TLS or `-dev`), Produce with key routing, Fetch with partition fan-out, SyncPull (subscriber with rebalance-aware partitioning), offset commit. In-process integration tests (bufconn) + example in `docs/developer.md`. No new dependencies beyond `google.golang.org/grpc` (already in tree).
2. **Slice B — `firn archive export`** (`firn-cli archive export --topic t --out dump.jsonl`): topic + offsets dump, no storage semantics change (read-only path via gRPC Fetch). Import deliberated separately.
3. **Slice C — Browser session auth for `/ui/`**: `POST /ui/session` with bearer → HttpOnly cookie (SameSite=Strict, Secure); `/ui/*` accepts cookie; expiry 12h; audit on grant/revoke. **Delivered 2026-11-13** (plan 005 slice 7).
4. **Future slices** (each its own ADR + storage review): format version byte; S3 tier; encryption at rest; import half of archive CLI; Python/Java/.NET SDKs if demand materializes.

## Implementation notes

- Slice A (`sdk/firngo`) **delivered 2026-11-13** — connect (TLS with caller-provided CA pool or system roots via `DialProduction`; plaintext only for `-dev` brokers via `DialInsecure`), produce with key routing, fetch, `SubscribePartition`, `CommitOffset`, `Health`, `TopicMetadata`, `JoinGroup`. Example in `docs/developer.md`; tests: in-process integration against a real broker, `-race` clean.
- Slice B (`firn archive export`) — not landed yet; will be implemented with plain `Fetch` pagination, no storage semantics changed.
- Slice C — **delivered 2026-11-13** (plan 005 slice 7). `POST /ui/session` (bearer or existing session) → HttpOnly, SameSite=Strict, `Path=/ui/` cookie, 12h TTL, `Secure` on TLS/`X-Forwarded-Proto: https`. `/ui/*` accepts bearer or cookie; the cookie is scoped away from the admin JSON API; per-key rate limit and `allowed_ips` apply to cookie auth; every cookie verification re-checks the key store so revoking a key kills its sessions ("revoke on key revoke"); sessions are in-memory only (restart = sign out); tokens stored SHA-256-hashed; grant/revoke audit-recorded; federation keys refused (403). See `docs/api.md` ("`POST /ui/session`") and `docs/operator.md`.

## Consequences

- Every unbuilt claim is now either delivered (flip the checkbox in this register) or tracked here with an honest status — no silent "coming soon".
- Client-side teams no longer need to read `firn.proto` to produce a message; the Go path is SDK-supported, others use `firn-cli` or generate stubs from the proto.
- The register is the canonical backlog; new feature ideas append here first, promoted to a numbered plan/ADR only when planned.

## Addendum (2026-11-19): integrations register (ADR-016/017/018 delivered)

The ingress/egress/pipeline ADRs (016–018) were accepted 2026-11-19 and delivered in reference form. Register of what now works and what deliberately does not:

- [x] **HTTP(S) ingest gateway** (`firn-ingest -http`) — bearer/HMAC auth, rate limit, DLQ, `202`-after-commit (ADR-016, plan 007).
- [x] **MQTT 3.1.1 publish-only IoT ingress** (`firn-ingest -mqtt`) — minimal stdlib server; subscriptions refused; retained forwarded as regular records (ADR-016, plan 007).
- [x] **Tail-file source connector + checkpoint runtime** (`firn-ingest -source-file`) — reference connector with byte-offset checkpoints (ADR-016, plan 007).
- [ ] **Kafka/NATS/Redpanda source connectors** — dependency-gated (no driver added); plan 007 slice 4 remainder.
- [ ] **SQL CDC (Postgres logical replication) / NoSQL change-stream connectors** — dependency-gated; plan 007 slice 5.
- [x] **File/object-stage sink** (`firn-egress -config …` `type: file`) — JSONL + manifest, idempotent re-delivery, pre-Databricks-staging shape (ADR-017, plan 008).
- [x] **HTTP(S)/IoT webhook sink** — `Idempotency-Key`, bearer/HMAC, retry budget, 4xx→DLQ (ADR-017, plan 008).
- [x] **S3-compatible sink** (stdlib SigV4 PUT, stub-tested, ADR-017 addendum 2026-11-19b) and **direct Databricks UC Volumes Files API sink** (stdlib, ADR-017 addendum 2026-11-19b) — both delivered 2026-11-19 without new dependencies. Kafka sink remains dependency-gated (plan 008 slice 4).
- [x] **Route lag surface + `firn-cli integrations` (validate, lag)** — `firn_egress_lag_records` gauge, `firn-egress -lag`, delivered 2026-11-19. Mutation commands deliberately not built (no control plane).
- [x] **Declarative pipeline engine** (`firn-pipeline`) — filter/project/map/enrich(JSONL)/route; single input; DLQ lineage (ADR-018, plan 009).
- [x] **AI-assisted stage (opt-in)** — `mock` + OpenAI-compatible providers via stdlib HTTP; contract-validated output; per-minute cap; daily circuit breaker; HTTPS+env-only keys (ADR-018, plan 009).
- [x] **Design-time mapping assistant (slice 5)** — `firn-pipeline -suggest-map`, delivers a proposed fragment for human review; shipped 2026-11-19.
- [x] **Multi-source pipelines** — `inputs` list with per-input groups and `_source` injection; correlation only via `enrich`, ordering across inputs undefined. Shipped 2026-11-19.
- [x] **Prometheus metrics for integration binaries** — `-metrics-addr` on all three binaries; records in/out, DLQ, op latency, MQTT connections, egress lag gauge. Shipped 2026-11-19.
