# Plan 003: Authentication, mTLS & admin UI (ADR-007, phase E)

Derived from `docs/plans/002-ha.md` phase E. Scope for v1 landing in this plan run:

1. **API keys** — `internal/apikeys`: SHA-256-hashed keys stored in the internal `__api_keys` topic (shared FileLog via the broker, replicated). Roles: `viewer`, `operator`, `admin`. Plaintext returned exactly once at creation.
2. **Enforcement** — gRPC unary interceptor on `EventingService` and `ReplicaService` + HTTP middleware on admin routes: keys required when any exist; open when none (dev default, documented). Role-gated verbs: viewer < operator < admin.
3. **mTLS** — `-tls-cert` / `-tls-key` flags for both listeners; `-tls-ca-cert` on the CLI; quorum transport unchanged (in-mTLS mesh already).
4. **Rate limit** — per-key token bucket (default 100 ops/s, configurable via flag), enforced at the same interceptors, not at the broker core.
5. **Audit** — `internal/audit` appends records to `__audit-0` for every admin mutation (topic create, key create/revoke, group reset, DLQ redrive, auth failure). Replicated via the block path automatically.
6. **Admin UI** — `internal/adminui`: read-only HTML page at `/` (healthz, topics, groups, replicas) using stdlib `html/template`; no new dependencies.

Out of this run (documented as follow-ups): OIDC/JWT federation, key rotation UI, UI interactivity, mTLS for the broker-broker mesh (bootstrap problem), per-key per-topic ACLs.

## Sequencing

| Step | Deliverable | Gate |
|---|---|---|
| 1 | `internal/apikeys` + shared handle | unit tests |
| 2 | gRPC/HTTP enforcement + role gates | integration test |
| 3 | mTLS flags + CLI wiring | smoke test |
| 4 | Rate limit | unit test |
| 5 | `__audit` topic + audit calls | restart durability test |
| 6 | `internal/adminui` at `/` | manual UX check |
| 7 | Auth README + config docs | docs review |

## Status (2026-11-13): phase E landed

All seven steps delivered and verified live:

- `internal/apikeys` — file-backed hashed store on the broker-owned shared `__api_keys` FileLog; plaintext shown once; roles viewer/operator/admin.
- Enforcement — HTTP middleware on the admin mux + gRPC unary interceptor; keys required once any key exists (open before bootstrap); `/healthz` and `/metrics` stay public; `/apikeys*` admin-gated, produce/queue ops operator-gated.
- mTLS — `-tls-cert`/`-tls-key` on both listeners; CLI `-ca-cert` (CA pool for both gRPC dial and HTTP admin client); verified with SAN cert over HTTPS + TLS gRPC. Tailscale-style deployments should front a TLS-terminating proxy or use a CA that signs client IPs.
- Rate limit — per-key token bucket (default 100/s, `-rate`), enforced in both middleware paths; `Unknown-or-rate-limited` budgets are audit-logged.
- Audit — `__audit` records topic-create(-proposed)/create/revoke + every authz refusal; replicated via the block path. View payloads by decoding the file (see docs/api.md).
- Admin UI — read-only page at `/ui/` listing topics and consumer groups; no new deps.
- Live smoke (both plain and TLS): 401 without key, 403 for under-privileged role, 200/201 with proper role, produce/fetch roundtrip through routed TLS gRPC, audit trail persisted under `__audit-0/`.

Follow-ups tracked but not in scope: OIDC/JWKS, rotation UI, ACLs, HA-replica visibility in UI, keyless-dial rejection tests in CI (manual smoke suffices today).
