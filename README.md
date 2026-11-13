# firn

A from-scratch event broker in Go providing **streaming** (topics, partitions, offsets, consumer groups) and **queue** (ack, redelivery, DLQ) semantics over a shared append-only log.

Status: **v0.4.0 — Firn, the durable event broker (secure by default, MIT).** See [`CHANGELOG.md`](CHANGELOG.md) and [`docs/adr/`](docs/adr/). License: [MIT](LICENSE).

## Documentation

- [`docs/install.md`](docs/install.md) — build, single-node quickstart, TLS + auth bootstrap, 3-node cluster, what persists where
- [`docs/configuration.md`](docs/configuration.md) — every flag, environment variables, retention rules, startup/recovery order
- [`docs/api.md`](docs/api.md) — HTTP admin + gRPC `FirnService` reference with request/response examples, error codes, CLI↔API map
- [`docs/operator.md`](docs/operator.md) — platform operator guide: install, network, sizing, storage integrations, all `firn-cli` commands, UI screens, runbooks
- [`docs/developer.md`](docs/developer.md) — client guide: publish/consume from apps (Go, Python, Java, .NET samples), auth, best practices, troubleshooting
- [`docs/sre.md`](docs/sre.md) — observability: metrics catalog, Prometheus scrape, alerting rules, logs, dashboards, on-call workflows
- [`docs/personas.md`](docs/personas.md) — who does what: operator, SRE, developer, security auditor, incident commander, release manager

## Quick start

```sh
make build   # go build ./...
make verify  # go vet ./... && go test ./...
go run ./cmd/firnd   # HTTP admin/health on :9460, gRPC on :9461; data persisted in ./data
```

```sh
curl localhost:9460/healthz
curl localhost:9460/metrics   # Prometheus exposition (ADR-006)
curl -X POST localhost:9460/topics -d '{"name":"orders","partitions":3}'
curl localhost:9460/topics
```

**Status: v0.1.0 — MVP (slices 1–6).** See [`CHANGELOG.md`](CHANGELOG.md) for the release notes and the ADR-005 acceptance checklist. `/metrics` is served on the admin port; logs are JSON via `log/slog`; `firn-cli` wraps the same APIs (`create-topic|create-queue|list|produce|fetch|groups|dlq|create-apikey|revoke-apikey|list-apikeys`).

## Queue mode (slice 5)

```sh
# create a queue topic (produces with the usual produce path; consumers Pull+Ack)
curl -X POST localhost:9460/topics -d '{"name":"jobs","partitions":1,"mode":"queue"}'

# pull up to 10 messages (30s default visibility lease; attempts tracked in __queue_attempts)
curl -X POST localhost:9460/queues/jobs/pull -d '{"max_messages":10,"visibility_timeout_ms":30000}'

# ack a delivery
curl -X POST localhost:9460/queues/jobs/ack -d '{"delivery_id":"<id from pull>"}'
```

Unacked messages are redelivered after the visibility lease expires; after 3 delivery attempts a message is diverted to `jobs.dlq` (auto-created stream topic). Guarantee: at-least-once, per-message, ordering best-effort.

**Status: slice 5 — queue mode.** Earlier slices: consumer groups (slice 4): `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`, range+roundrobin assignment, member lease expiry; committed offsets in `__consumer_offsets` survive restart, membership does not. HTTP admin: `GET /groups`, `GET /groups/{name}/offsets`.

**Status notes:** earlier slices — file log with CRC recovery (slice 2): records are CRC-validated frames in segmented logs under `-data-dir` (default `./data`, `-data-dir=""` = in-memory), `kill -9`-tested torn-tail recovery; whole-segment retention is opt-in via `-retention-ttl` / `-retention-max-bytes`; offsets below the retention horizon return `FailedPrecondition`.

## Regenerating the protobuf stubs

```sh
buf generate internal/api --template buf.gen.yaml   # needs buf + protoc-gen-go + protoc-gen-go-grpc
```

## Running a 3-node cluster (ADR-007, experimental)

```sh
# terminal 1
go run ./cmd/firnd -node-id n1 -peers n1=127.0.0.1:9538,n2=127.0.0.1:9540,n3=127.0.0.1:9542 \
  -http-addr 127.0.0.1:9537 -grpc-addr 127.0.0.1:9538 -data-dir ./data-n1
# terminal 2 / 3: same with n2/n3 and ports 9540/9542 + 9539/9541, -data-dir ./data-n2 / ./data-n3
```

Topics and their blocks replicate via an embedded Raft quorum (no ZK/etcd). The client
(`firn-cli`) needs the node list:

```sh
firn-cli -peers 127.0.0.1:9538,127.0.0.1:9540,127.0.0.1:9542 \
  -http-peers 127.0.0.1:9537,127.0.0.1:9539,127.0.0.1:9541 \
  create-topic --name orders --partitions 3
firn-cli -peers ... produce --topic orders --key k --value hello
firn-cli -peers ... fetch --topic orders --partition 0
```

Design: [`docs/adr/007-scaled-deployment.md`](docs/adr/007-scaled-deployment.md) — block log, lease tails, embedded metadata quorum.

**Status: cluster mode (ADR-007) is experimental and lives behind `-node-id`/`-peers`. Caveats:** (1) lease re-grants land on the current quorum leader, not a re-balanced owner — expect leader affinity after failovers; (2) unsealed tails (≤ `--lease-ttl`/`SealInterval`) are the at-risk window; (3) membership is currently fixed at startup — no dynamic join/leave; (4) `GetMetadata` serves routing hints but the CLI still tolerates stale hints with retry; (5) internal-topic writes (`__consumer_offsets`/`__queue_attempts`/`__audit`/`__api_keys`) are single-writer-fenced by the internal lease — non-holder nodes fail closed (HTTP 503 / gRPC FailedPrecondition) and apps/CLI retry the holder.

## Secure by default (v0.4.0)

**A fresh broker serves only `/healthz` until you mint the first API key** via the one-time bootstrap token printed on stderr (or restart with `-dev` to re-enable open dev mode, which is loud). /metrics is keyed; mesh RPCs live on a dedicated mTLS listener; data dirs are 0700/files 0600; anonymous traffic is rate-limited per IP; audit logging is bounded and append-only. See [`docs/migration-v0.3-to-v0.4.md`](docs/migration-v0.3-to-v0.4.md).

## Authentication, TLS & admin UI (phase E, 2026-11-13)

Firn is **secure by default**: a fresh daemon with zero keys refuses to serve every endpoint except `/healthz`. On first start it prints a one-time `BOOTSTRAP TOKEN` to stderr; `POST /apikeys` with that token mints exactly one admin key, then bearer auth enforces everywhere except `/healthz`. `-dev` re-enables open mode for development, loudly. Restarting with existing keys enforces immediately; the bootstrap token is never reprinted and cannot be replayed. Keys are SHA-256-hashed and stored in the replicated `__api_keys` internal topic; roles `viewer` < `operator` < `admin`; per-key token-bucket rate limit. Admin mutations are recorded in the replicated `__audit` topic. `-tls-cert`/`-tls-key` serve both listeners over TLS (min TLS 1.2); the CLI dials HTTPS/TLS gRPC with `-ca-cert`. The read-only admin UI is at `/ui/`.

```sh
# bootstrap: create the first (admin) key before serving traffic
curl -X POST localhost:9460/apikeys -d '{"name":"root","role":"admin"}'
firn-cli -api-key 'ev-…' list
```

Details: [`docs/configuration.md`](docs/configuration.md) (flags, auth-enforcement model), [`docs/api.md`](docs/api.md) (key routes, roles, error codes), [`docs/plans/003-auth.md`](docs/plans/003-auth.md) (delivery notes).

## Multi-cluster federation (preview, ADR-010)

Two+ Firn clusters can replicate topics and queues between them over mTLS + a federation-scoped key. Mode is per peer: **passive** (one-way DR — sink stays fenced, offsets preserved byte-identically, until the operator cuts over) or **active-active** (both clusters accept writes; records are re-originated per cluster with fresh local offsets, ts/key/value preserved, echo filtered server-side). Selective per-topic/per-queue scoping; federation keys can call federation RPCs and nothing else; `firn federation add-peer|status` for operations; `firn_federation_cursor_lag_seconds` metric; `/ui/federation` read-only view. Two-cluster registration example: [`docs/install.md`](docs/install.md#two-cluster-federation-preview-adr-010) · semantics: [`docs/adr/010-federation.md`](docs/adr/010-federation.md).

## Design docs (read in order)

1. [ADR-001 Scope](docs/adr/001-scope.md)
2. [ADR-002 API surface](docs/adr/002-api.md)
3. [ADR-003 Storage engine & invariants](docs/adr/003-storage.md)
4. [ADR-004 Consistency stance](docs/adr/004-consistency.md)
5. [ADR-005 MVP definition](docs/adr/005-mvp.md)
6. [ADR-006 Metrics](docs/adr/006-metrics.md)
7. [ADR-007 Scaled-out deployment (proposed→in progress)](docs/adr/007-scaled-deployment.md)
8. [ADR-008 Documentation](docs/adr/008-documentation.md)
9. [ADR-009 Storage solutions validation](docs/adr/009-storage-solutions.md)
10. [ADR-010 Federation (multi-cluster replication)](docs/adr/010-federation.md)
11. [ADR-011 Secure-by-default mode](docs/adr/011-secure-mode.md)
12. **[ADR-012 Architecture — components & deployment topologies](docs/adr/012-architecture.md)** — start here for the system map (standalone, HA cluster, active/passive and active/active federation)

Working with coding agents on this repo: read [`AGENTS.md`](AGENTS.md) — storage invariants are mandatory context, and storage diffs always need human review.

## License

[MIT](LICENSE) — Copyright (c) 2026 rahulkj and contributors.
