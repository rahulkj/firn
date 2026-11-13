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
