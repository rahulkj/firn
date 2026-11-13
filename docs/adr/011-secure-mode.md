# ADR-011: Secure-by-default mode — defending Firn clusters exposed to the internet

Status: accepted · 2026-11-13 · Implementation via `docs/plans/005-security.md`. Storage-adjacent review gates apply to mesh-transport changes; every slice lands with its own gate.

## Context

Firn clusters run on machines with public or semi-public IPs. Automated scanners and targeted bots routinely probe exposed brokers for data exfiltration (customer event payloads, PII in records, internal topology). The threat model:

- **In scope**: internet-exposed instances; unauthorized read/write; credential theft; mesh eavesdropping/injection; pivot from exposed admin surface to full cluster; denial of service by volume.
- **Out of scope**: nation-state actors with physical access; compromised CA/issuer itself; insider with OS root on hosts (that already owns the data-dir and `topics.json` regardless of what this ADR buys); side-channel attacks on crypto implementations.

**Not legal advice.** This is engineering-defense-in-depth, and it will only be as good as its weakest default.

## Findings (audited 2026-11-13, against this tree)

| # | Finding | Evidence |
|---|---|---|
| F-1 | **Fresh installs with zero API keys are fully open** — produce, fetch, admin mutations, `ReplicaPut`/`ReplicaFetch` over the client port, no credential required | `internal/api/auth.go` `enforced()` returns false until the first key exists; dev default never creates keys |
| F-2 | **Quorum mesh transport is plaintext gRPC with zero credentials** — any host on the network segment can dial the quorum port and `ReplicaFetch` any full topic | `internal/quorum/transport_grpc.go` uses `insecure.NewCredentials()` and no interceptors |
| F-3 | **Mesh RPCs inherit the public-facing role table with no floor** — `QuorumMessage` is unmapped (defaults to `viewer`); `ReplicaFetch` needs `viewer`; pre-bootstrap, no key needed at all | `internal/api/auth.go` `grpcRoles` map |
| F-4 | `/metrics` is public by default; `/healthz` is public by design (documented trade-off) | `internal/api/auth.go` `routeTable.public` |
| F-5 | Anonymous traffic (healthz, metrics, unauth refusals) is unlimited in rate; no per-key IP/CIDR allowlist | `internal/ratelimit` is key-only; no global anon bucket |
| F-6 | Data directory permissions are not enforced at startup; `__api_keys` hashes live world-readable in default FS perms | `internal/broker`, `internal/meta` `OpenRegistry`, `internal/log.Open` |
| F-7 | `__audit` has no rotation/retention bound and records are never rotated; authz refusals vanish silently when not enforced (open mode) unless audit is wired | `internal/audit/audit.go`, `cmd/firnd` wiring |
| F-8 | Federation allows `allow_plaintext`; mTLS for the client mesh recommended but **optional** (and mesh is plaintext today per F-2) | ADR-010 §security; `internal/replication/advisor.go` |
| F-9 | No SBOM, no artifact signing, no vuln scan gate on releases | `.github/workflows`, `dist/release-vX` |
| F-10 | HTTP admin responses lack hardening headers (CSP, X-Frame-Options, Referrer-Policy); UI is HTML from template (escaped) but no headers at all | `internal/api/logging.go`, `internal/adminui/ui.go` |

## Principles

1. **Fail closed by default.** A daemon that could not turn on auth is less dangerous than a daemon that forgot to. Opt-out must always be explicit (`-dev`, `-allow-open-bootstrap`) and must look dangerous on purpose in logs.
2. **Least privilege.** Mesh identity ≠ API key; an admin key must not be able to slurp every partition if a single-token replica ops account would do.
3. **Smallest possible surface.** Public = `/healthz` only. Everything else requires a credential, shipped with a per-key rate limit and a global anonymous limit.
4. **No secret at rest in plaintext** — already true for API keys (SHA-256 hashes), to be extended: audit logs get write-once, read-only-inside-cluster.
5. **Auditable by default.** Refusals must be recorded in every mode including open.
6. **No new complexity in the hot path.** Security must not regress payload-path latency materially.

## Decision

> **Implementation status (2026-11-13): slice 1 delivered** — fail-closed startup + bootstrap token as described below (D-001, D-002, D-004; `/metrics` keyed by default). Remaining findings are tracked in plan 005 slices 2–6.


**D-001 — Secure-by-default startup.** `firnd`/`firnd` refuses to bind any listener unless either (a) at least one non-revoked API key exists in `__api_keys`, or (b) the operator passes `-allow-open-bootstrap` (intentionally named). `-dev` is an alias for `-allow-open-bootstrap` for local work. `/healthz` becomes the only anonymous endpoint even in bootstrap/open mode; `/metrics` is removed from the default-public set — operators who want unauth scraping must pass `-metrics-auth=false` explicitly (default: `/metrics` requires a `viewer` key).

**D-002 — One-time bootstrap token.** On first start with zero keys, the daemon prints a one-time token to stderr (`BOOTSTRAP TOKEN: …`). That token authorizes exactly one key creation (`POST /apikeys` with the token as the bearer) or is burned at first use. Subsequent keys are created by authenticated admin. This erases the "fresh deploy is open for N minutes" window even before any key exists.

**D-003 — Mesh authentication.** All internal gRPC RPCs (`ReplicaService`, `QuorumService`, and same-cluster `FederationService`) require a **mesh identity** (mTLS client cert chaining to the cluster CA), not an API key. The public-facing interceptor no longer applies to mesh RPCs; instead a mesh interceptor:
  - verifies the peer cert chain against the cluster CA (same CA used for federation),
  - assigns internal role `mesh`,
  - refuses any mesh call from a non-mesh identity with `Unauthenticated`.
The public interceptor (`grpcRoles`) and `routeTable` are unchanged for external callers. Once D-003 lands, F-2 and F-3 are closed without breaking bootstrap (mesh bootstrap = CA distribution, operator-controlled, out of band).
Mixed old/new clusters are **unsupported**: rolling upgrade is not safe for the mesh; take a brief maintenance window or run parallel mesh with per-cluster CA.

**D-004 — Anonymous traffic is bounded.** New flag `-anon-rate` (default 20 ops/s per IP, sliding window) applies to `/healthz` and any unauth endpoint; exceeded → `429`. `-metrics-auth` defaults to `true` (see D-001).

**D-005 — Per-key IP allowlist.** `POST /apikeys` accepts optional `allowed_ips` (CIDR list). The key is then accepted only from those CIDRs; refusals are audit-recorded. Absent `allowed_ips` = unrestricted (unchanged behavior for existing keys after migration).

**D-006 — Data directory is private by construction.** At open, `internal/log.Open` and `internal/meta.Open` create files with `0600` and the data dir with `0700` and refuse to start if they can write but the dir is not private enough (configurable `-dir-mode-check=false` for exotic filesystems, logged loudly). Existing dirs tightened on next open.

**D-007 — Audit is always on, bounded, and append-only.**
  - Refusals are audited **in every mode including open** (audit always wired at startup whether keys exist or not).
  - New flag `-audit-max-bytes` (default 64 MiB); on overflow the daemon rotates `__audit-N` (N = 1..9), preserving append-only within each file, and refuses new admin mutations (fail-closed for writers) until space frees. Read-only paths continue to work.
  - Document that audit is not tamper-evident against insider OS root; it exists to catch unauthorized access from outside, not to police hosts.

**D-008 — Metrics and UI are gated by default.** `/metrics` requires a viewer key by default (D-001). `/ui/` returns 403 without a viewer key (already the behavior behind `authorizer.Wrap`). No CSP loosening; `default-src 'self'`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, `Cache-Control: no-store` on `/ui/` and admin JSON responses.

**D-009 — Federation plaintext is removed.** `allow_plaintext: true` from ADR-010 is withdrawn; federation registration requires `ca_cert` (or the cluster CA) unconditionally. Self-certified peers are supported by pinning a specific cert, never by disabling TLS. Document the migration: federation registration re-run with CA.

**D-010 — Distribution hardening.** Releases carry: SPDX SBOM (via `syft` in CI), cosign-signed tarballs (`cosign sign-blob`), `grype` fail-on-critical gate, and a `SHA256SUMS` with SLSA-style attestation line. These are CI-side artifacts; no runtime dependency added to the shipped binary.

**D-011 — Encryption at rest is explicitly deferred**, not included. It needs its own threat model (KMS vs local passphrase vs HSM; performance on the fsync path; key distribution for federation) and belongs in ADR-012. This ADR gets you **without-kms** defense-in-depth, not FIPS.

## Consequences

- **Breaking change for existing default installs**: a `firnd` with zero keys will no longer listen. Migration: set one admin key (restore from backup or create new), restart with `-allow-open-bootstrap` once to recreate, or use the bootstrap token on next start with zero keys. Document as the v0.4.0 upgrade note, not a silent behavior change.
- **Mesh upgrade is a maintenance-window operation**, not rolling. Plan coordinator for clusters ≥ 2 nodes before enabling.
- **Performance**: mesh mTLS adds cert-verify per message (short-lived connections today). Expected overhead small; benchmark target before release: ≤ 5% end-to-end regression on the produce path (bench gate).
- **Operator ergonomics**: admin UIs now require auth end-to-end; healthz/scraper recipes change (pass a viewer key header); documented in install/config and the release note.
- **Audit grows**: audit volume rises (refusals are always written); `-audit-max-bytes` exists to keep the disk bounded; operators should grep `authz-refused` on port 80/443 scans.
- Zero-key fresh deploys in CI/e2e need `-dev` or a seeded key — documented; tests updated.

## Non-goals

- Multi-tenancy (hard per-topic ACL, tenant isolation) — later ADR if requested.
- Encryption at rest (D-011); FIPS-validated crypto; HSM-backed keys.
- Intrusion detection, WAF, SIEM integration, IDS alert wiring — logs are ours, monitor is yours.
- Secret rotation UI. Keys rotate by operator (delete/create); UI is unchanged.
- Airgapped/offline distribution of keys — registry is whatever operator does out of band.

## When this plan changes

Moving any item out of scope of this ADR requires a new ADR and never a silent weakening in a release. D-011 explicitly cannot be "snuck in" later without its own review.
