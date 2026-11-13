# Plan 005: Secure-by-default mode (ADR-011)

Derived from ADR-011. One slice per session, same loop as plans 001–004:

> paste the slice spec → implement → tests first → read the diff → `go vet ./... && go test ./... -race` green → human review for quorum/storage-adjacent diffs → commit.

Non-negotiables: storage/quorum diffs always human-reviewed; plan changes land here first; slice 6 ships with ADR validation + docs, never silent scope growth.

## Slices

| Slice | Delivers | Review gate |
|---|---|---|
| 1 (**delivered 2026-11-13**, review pending) | **Secure-by-default + bootstrap token**: fail startup without a key (or `-allow-open-bootstrap`/`-dev`); one-time bootstrap token on stderr; `POST /apikeys` with that token creates the first key then token is void; `/healthz` only anonymous endpoint | API review (bootstrap path) — [GO/NO-GO smoke evidence below](#slice-1-evidence) |
| 2 (**delivered 2026-11-13**) | **Mesh mTLS**: separate mesh gRPC listener; shared cluster CA required in cluster mode (provisioned once via `firn-cli mesh-ca init`, copied to each node), auto-provision single-node only; ReplicaService/QuorumService served only on the mesh port behind mTLS; public listener never exposes them | **mandatory (quorum transport + storage-adjacent)** — storage review pending |
| 3 (**delivered 2026-11-13**) | **Classification tightening**: `/metrics` now requires viewer key by default; `-metrics-auth=false` escape hatch; HTTP hardening headers on admin/UI responses (CSP, XFO, Referrer-Policy, no-store); `/ui/` unchanged behavior (already keyed) | API review |
| 4 (**delivered 2026-11-13**) | **Rate limiting everywhere**: `-anon-rate` global per-IP sliding window on unauth endpoints; per-key `allowed_ips` CIDR allowlist persisted in `__api_keys`; refusals audit-recorded in **all** modes | API review |
| 5 | **Storage private**: data dir `0700`/files `0600` enforced at open (warn+continue only under `-dir-mode-check=false`); `-audit-max-bytes` rotation; audit always wired; `authz-refused` always recorded | **mandatory (storage-adjacent)** |
| 6 | **Distribution hardening**: SBOM (`syft`), cosign-signature of release tarballs, `grype` fail-on-critical CI gate; release notes; install/config docs updated; migration guide v0.3→v0.4 | docs review |

## Slice details

### Slice 1 — secure startup & bootstrap

- Startup check: zero keys + `!allowOpenBootstrap && !devFlag` → `slog.Fatal` before listeners bind.
- Bootstrap flow: zero keys → `stderr` prints `BOOTSTRAP TOKEN: <random>`; `POST /apikeys` with bearer == token → creates key (role admin), token invalidated, audit record, daemon continues normally.
- `-dev` implies `-allow-open-bootstrap`; both logged at startup as `INSECURE BOOTSTRAP ENABLED` warning.
- Tests: refuses to start without keys; starts with token + creates key via token; refuses token reuse; refuses token after key exists; `/healthz` answers without auth; `/metrics` now `401`.

### Slice 2 — mesh mTLS (rolling maintenance)

- Cluster CA generated once under `<data-dir>/mesh-ca/` (files `0600`) on first start (028 = fsync parent), or provided via `-mesh-ca-bundle`/`-mesh-ca-key` for operator-managed CAs.
- Each node gets a cert signed by the CA on first start (CSR local, CN = node-id); `-mesh-ca-*` override.
- `TransportGRPC` dial uses mesh CA + its own cert; `QuorumService` handlers verify peer chains to mesh CA.
- Internal RPCs (`ReplicaService`, `QuorumService`) drop out of `grpcRoles`/`routeTable`; new mesh interceptor enforces chain-to-CA identity; public interceptor untouched.
- Rolling upgrade: **not supported in-place** (documented); intended for fresh clusters or coordinated one-shot restart.
- Gate: benchmark target ≤ 5% regression on produce path vs post-v0.3 baseline.

### Slice 3 — classification + headers

- `/metrics` now `401` without a key; `-metrics-auth=false` restores unauth metric scrape (documented; not recommended).
- Response middleware adds: `Content-Security-Policy: default-src 'self'`; `X-Frame-Options: DENY`; `Referrer-Policy: no-referrer`; `Cache-Control: no-store`. Applied to admin JSON + `/ui/` only (healthz/metrics excluded deliberately).
- Tests: headers present; `/metrics` 401 without key, 200 with viewer key.

### Slice 4 — rate limits & allowlists

- `-anon-rate` (default 20 ops/s per IP) for `/healthz` and unauth refusals; 429 with body `{"error":"rate limited"}`.
- `allowed_ips` in apikeys: stored per-key; CIDR parse+match; misses → 403 (or 401 — decide by slice review; prefer 403 for "valid key, wrong IP") with audit `authz-refused:ip-not-allowed`.
- Audit in **all modes**: `authz-refused` always lands in `__audit`, including open mode.

### Slice 5 — storage private

- `FileLog`/`OpenRegistry` create parent dirs `0700`, files `0600`; refuse to start on mode mismatch unless `-dir-mode-check=false` (CI/heterogeneous filesystems); `topics.json` same.
- `-audit-max-bytes` (default 64 MiB): rotate `__audit` → `__audit-1..N`; writes fail closed (503) past budget; reads unaffected; operator reclaims disk.
- Audit always wired: `cmd/firnd` wires audit before serving in both open and enforced mode; repro tests for `authz-refused` under open mode.
- Gate: storage review for open/compact paths (these are storage-adjacent).

### Slice 6 — distribution hardening & docs

- CI workflow: `syft` SBOM (`packages=…` emitted per release), `cosign sign-blob` over each tarball, `grype` critical-only gate; no new runtime deps.
- Docs: `docs/install.md` "secure by default" callout; `docs/configuration.md` gains every new flag + classification table; `docs/api.md` notes 401/403/429 shapes for every endpoint; README status updated.
- Migration note v0.3→v0.4: zero-key installs must create a key via bootstrap token or `-dev` before first start after upgrade.
- Release notes flag mesh rolling-restart requirement.

## Open items (tracked, not blocking)

- `-mesh-ca-bundle`/`-mesh-ca-key` operator-managed CA flow — details deferred to slice 2 implementation; default self-generated CA ships.
- Encryption at rest (ADR-011 D-011) — ADR-012 candidate, not this plan.
- mTLS for federation: **decided already (ADR-010 + 011 D-009)** — no further work needed beyond what federation plan 004 covers.
- Key rotation UX — out of scope for v1 here; operator rotates via create+revoke.
