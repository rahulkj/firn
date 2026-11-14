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
- [ ] **Browser-native auth for `/ui/`** — UI is read-only; browsers can't send bearer headers, so `/ui/` behind keyed auth requires a proxy with a pinned viewer key, or `-dev` locally. A session-cookie flow (exchange bearer → HttpOnly cookie) is the obvious fix; small but touches auth middleware. Candidate slice below.
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
3. **Slice C — Browser session auth for `/ui/`**: `POST /ui/session` with bearer → HttpOnly cookie (SameSite=Strict, Secure); `/ui/*` accepts cookie; expiry 12h; audit on grant/revoke.
4. **Future slices** (each its own ADR + storage review): format version byte; S3 tier; encryption at rest; import half of archive CLI; Python/Java/.NET SDKs if demand materializes.

## Implementation notes

- Slice A (`sdk/firngo`) **delivered 2026-11-13** — connect (TLS with caller-provided CA pool or system roots via `DialProduction`; plaintext only for `-dev` brokers via `DialInsecure`), produce with key routing, fetch, `SubscribePartition`, `CommitOffset`, `Health`, `TopicMetadata`, `JoinGroup`. Example in `docs/developer.md`; tests: in-process integration against a real broker, `-race` clean.
- Slice B (`firn archive export`) — not landed yet; will be implemented with plain `Fetch` pagination, no storage semantics changed.
- Slice C — not landed; design note: bearer exchange endpoint, short-lived HttpOnly cookie with SameSite=Strict, revoke on key revoke.

## Consequences

- Every unbuilt claim is now either delivered (flip the checkbox in this register) or tracked here with an honest status — no silent "coming soon".
- Client-side teams no longer need to read `firn.proto` to produce a message; the Go path is SDK-supported, others use `firn-cli` or generate stubs from the proto.
- The register is the canonical backlog; new feature ideas append here first, promoted to a numbered plan/ADR only when planned.
