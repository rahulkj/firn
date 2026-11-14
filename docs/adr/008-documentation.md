# ADR-008: Documentation & developer experience — install, configure, and use everything

Status: accepted · 2026-11-13 · Implementation delivered (see `docs/install.md`, `docs/configuration.md`, `docs/api.md`).

## Context

A broker nobody can install, configure, or even call is a paper design. Before this ADR, the repo had: a quickstart in the README, design ADRs, and code comments — but no installation guide, no configuration reference, and no API reference with request/response examples. Building a broker is not done until a developer can `make run` and produce a message in under five minutes.

## Decision

Create three developer-facing documents under `docs/`, covering install/config/use completely:

1. **`docs/install.md`** — build from source (Go 1.27+, `make`), single-node quickstart (produce + fetch in five minutes), three-node cluster quickstart, and a clear "what gets persisted where" map of the data directory.
2. **`docs/configuration.md`** — every `firnd` flag with defaults, types, and effects; cluster peer format; retention horizon rules (deletion never touches the active segment, never rewrites offsets); startup sequence and crash recovery order.
3. **`docs/api.md`** — every network surface: HTTP admin (`POST /topics`, `GET /topics`, `GET /groups`, `GET /groups/{name}/offsets`, `GET /metrics`, `GET /healthz`), gRPC `FirnService` (`Produce`, `Fetch`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `CommitOffset`, `FetchOffset`, `Pull`, `Ack`, `GetMetadata`), internal replication service (`ReplicaService`), quorum transport service (`QuorumService`), with JSON and protobuf request/response examples and an error-codes table.

Maintenance rule: any change to flags, routes, or RPCs updates these docs in the same commit as the code. The CI lint gate stays green.

### Out of scope

- SDK ergonomics (the `firn-cli` is the reference client; Go SDK lands with ADR-007 phase D-adjacent work).
- Rendered/published docs site; everything ships as markdown in the repo, one read away from the code.
- Benchmarks beyond `bench/baseline.md`.

## Validation

- All examples in `docs/install.md` were executed against a running single node and a 3-node cluster during this pass; outputs shown are real, not invented. The quickstart sequence (open mode, no keys) was re-run end-to-end on a fresh `-data-dir` at this commit: topic create via curl, produce routing, fetch round-trip.
- `docs/api.md` request/response samples were copied from live runs where possible and cross-checked against `internal/api/firn.proto`; the audit-log frame decoder was verified against a real `__audit-0` segment.
- During this pass the CLI was fixed to honor `-ca-cert` on the HTTP admin path (HTTPS + CA pool), not only on gRPC — before the fix `-ca-cert` silently left admin calls on plaintext.
- Single-node advertised address in `GetMetadata` is fixed to reflect `-grpc-addr` (wildcards fold to `localhost`); previously it advertised a hardcoded `localhost:9093`.

## Consequences

- New contributors and evaluators can stand the broker up and produce traffic without reading Go code.
- Documentation is now part of "done" for every feature change; stale docs are treated as bugs, not cosmetic issues.
- No new runtime dependencies; docs ship in-repo only.

## Implementation notes (2026-11-13): SDK still absent, ports updated

Two notes vs this record:
- "Go SDK lands with ADR-007 phase D-adjacent work" — phase D shipped (v0.2.0) without an SDK; nothing shipped since. `firn-cli` remains the reference client.
- Listener defaults moved from `:9092`/`:9093` to **`:9460`/`:9461`** (v0.4.0 BREAKING change); all cited examples in docs were switched to the new defaults; single-node `GetMetadata` now advertises unknown ("") instead of a hardcoded `localhost:9093`.

## Addendum (2026-11-13): full documentation set — all personas delivered

This ADR is expanded from three docs to the complete reference set, per the same decision record extended post-v0.4.0. **Every document below exists and is maintained under the same rule (update in the same commit as the code change):**

| Document | Audience | Covers |
|---|---|---|
| `docs/install.md` | everyone | tarball + from-source builds, secure bootstrap, single-node + 3-node + federated quickstarts |
| `docs/configuration.md` | operator | every flag, env vars, retention horizon rules, startup/recovery order |
| `docs/api.md` | developer + operator | every endpoint, request/response examples, error tables, CLI↔API map |
| `docs/operator.md` | **platform operator** | install paths, network segments + firewall matrix, sizing with measured benchmarks, storage integrations (local fs, disk sizing math, in-memory limits, S3 explicitly absent), all `firn-cli` commands, UI walkthroughs (real screenshots), secure-bootstrap , backup/restore runbook, upgrade notes |
| `docs/developer.md` | **application developer** | concepts, auth patterns for apps (keys, `allowed_ips`, bearer), verified publish/consume in Go, Python, Java (executed against a live broker), .NET shape documented, best practices, troubleshooting |
| `docs/sre.md` | **SRE / on-call** | metrics catalog with targets, Prometheus scrape, alert rules, dashboard spec, log drill-down, federation lag, backup/restore runbook |
| `docs/personas.md` | leadership/enablement | roles ↔ surfaces matrix (operator, SRE, dev, auditor, incident commander, release manager, data owner) |
| `docs/adr/012-architecture.md` | everyone | system map: component catalog, interactions, deployment topologies (standalone/HA/federated), security architecture, threat model |
| `docs/migration-v0.3-to-v0.4.md` | operator | upgrade notes for the v0.3 → v0.4 breaking window (ports, fail-closed boot) |
| `docs/distribution.md` | release manager | build, SBOM, grype, signing, verification checklist |
| `docs/federation-cutover.md` | operator | passive→active cutover procedure |

Additional maintenance-rule extensions:
- Benchmarks: `bench/baseline.md` carries the current numbers (v0.1.0 baseline preserved; v0.4.0 re-run appended below it).
- Screenshots: `docs/images/ui-dashboard.png` and `docs/images/ui-federation.png` are re-captured from a live demo cluster whenever `internal/adminui` changes.
- The `adminui` inline `<style>` was externalized to `/ui/style.css` so the UI renders in strict-CSP browsers.

Remaining gap (carried forward, honestly): **no client SDK is shipped** — `firn-cli` is the reference client; see ADR-013 addendum for the register of unbuilt features and their plans.

## Addendum (2026-11-13): SDK closure

The "SDK still absent" note is superseded: `sdk/firngo` delivered (connect/produce/fetch/subscribe/commit/Group). Developer docs include a Go SDK section; Python/Java/.NET docs remain stub-generation instructions until demand appears (see ADR-013).
