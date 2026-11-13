# slipway

A from-scratch event broker in Go providing **streaming** (topics, partitions, offsets, consumer groups) and **queue** (ack, redelivery, DLQ) semantics over a shared append-only log.

Status: **v0.2.0-preview — ADR-007 scaled deployment (behind `-node-id`/`-peers`), plus v0.1.0 MVP.** See [`CHANGELOG.md`](CHANGELOG.md) and [`docs/adr/`](docs/adr/).

## Documentation

- [`docs/install.md`](docs/install.md) — build, single-node quickstart, TLS + auth bootstrap, 3-node cluster, what persists where
- [`docs/configuration.md`](docs/configuration.md) — every flag, environment variables, retention rules, startup/recovery order
- [`docs/api.md`](docs/api.md) — HTTP admin + gRPC `SlipwayService` reference with request/response examples, error codes, CLI↔API map

## Quick start

```sh
make build   # go build ./...
make verify  # go vet ./... && go test ./...
go run ./cmd/slipwayd   # HTTP admin/health on :9092, gRPC on :9093; data persisted in ./data
```

```sh
curl localhost:9092/healthz
curl localhost:9092/metrics   # Prometheus exposition (ADR-006)
curl -X POST localhost:9092/topics -d '{"name":"orders","partitions":3}'
curl localhost:9092/topics
```

**Status: v0.1.0 — MVP (slices 1–6).** See [`CHANGELOG.md`](CHANGELOG.md) for the release notes and the ADR-005 acceptance checklist. `/metrics` is served on the admin port; logs are JSON via `log/slog`; `slipway-cli` wraps the same APIs (`create-topic|create-queue|list|produce|fetch|groups|dlq|create-apikey|revoke-apikey|list-apikeys`).

## Queue mode (slice 5)

```sh
# create a queue topic (produces with the usual produce path; consumers Pull+Ack)
curl -X POST localhost:9092/topics -d '{"name":"jobs","partitions":1,"mode":"queue"}'

# pull up to 10 messages (30s default visibility lease; attempts tracked in __queue_attempts)
curl -X POST localhost:9092/queues/jobs/pull -d '{"max_messages":10,"visibility_timeout_ms":30000}'

# ack a delivery
curl -X POST localhost:9092/queues/jobs/ack -d '{"delivery_id":"<id from pull>"}'
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
go run ./cmd/slipwayd -node-id n1 -peers n1=127.0.0.1:9311,n2=127.0.0.1:9312,n3=127.0.0.1:9313 \
  -http-addr 127.0.0.1:9211 -grpc-addr 127.0.0.1:9311 -data-dir ./data-n1
# terminal 2 / 3: same with n2/n3 and ports 9312/9313 + 9212/9213, -data-dir ./data-n2 / ./data-n3
```

Topics and their blocks replicate via an embedded Raft quorum (no ZK/etcd). The client
(`slipway-cli`) needs the node list:

```sh
slipway-cli -peers 127.0.0.1:9311,127.0.0.1:9312,127.0.0.1:9313 \
  -http-peers 127.0.0.1:9211,127.0.0.1:9212,127.0.0.1:9213 \
  create-topic --name orders --partitions 3
slipway-cli -peers ... produce --topic orders --key k --value hello
slipway-cli -peers ... fetch --topic orders --partition 0
```

Design: [`docs/adr/007-scaled-deployment.md`](docs/adr/007-scaled-deployment.md) — block log, lease tails, embedded metadata quorum.

**Status: cluster mode (ADR-007) is experimental and lives behind `-node-id`/`-peers`. Caveats:** (1) lease re-grants land on the current quorum leader, not a re-balanced owner — expect leader affinity after failovers; (2) unsealed tails (≤ `--lease-ttl`/`SealInterval`) are the at-risk window; (3) membership is currently fixed at startup — no dynamic join/leave; (4) `GetMetadata` serves routing hints but the CLI still tolerates stale hints with retry; (5) internal-topic writes (`__consumer_offsets`/`__queue_attempts`/`__audit`/`__api_keys`) are single-writer-fenced by the internal lease — non-holder nodes fail closed (HTTP 503 / gRPC FailedPrecondition) and apps/CLI retry the holder.

## Authentication, TLS & admin UI (phase E, 2026-11-13)

The broker enforces bearer-token auth on both surfaces once **any non-revoked API key exists** — open by default for bootstrap, locked down afterwards. Keys are SHA-256-hashed and stored in the replicated `__api_keys` internal topic; roles `viewer` < `operator` < `admin`; per-key token-bucket rate limit. Admin mutations are recorded in the replicated `__audit` topic. `-tls-cert`/`-tls-key` serve both listeners over TLS (min TLS 1.2); the CLI dials HTTPS/TLS gRPC with `-ca-cert`. The read-only admin UI is at `/ui/`.

```sh
# bootstrap: create the first (admin) key before serving traffic
curl -X POST localhost:9092/apikeys -d '{"name":"root","role":"admin"}'
slipway-cli -api-key 'ev-…' list
```

Details: [`docs/configuration.md`](docs/configuration.md) (flags, auth-enforcement model), [`docs/api.md`](docs/api.md) (key routes, roles, error codes), [`docs/plans/003-auth.md`](docs/plans/003-auth.md) (delivery notes).

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

Working with coding agents on this repo: read [`AGENTS.md`](AGENTS.md) — storage invariants are mandatory context, and storage diffs always need human review.

## License

[MIT](LICENSE) — Copyright (c) 2026 rahulkj and contributors.
