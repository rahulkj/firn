# Installing and running slipway

Slipway is a from-scratch event broker: **streaming** (topics, partitions, consumer groups, replay) and **queue** (ack, redelivery, DLQ) semantics over a shared append-only log. This page takes you from a clean checkout to a running broker with traffic flowing.

## Prerequisites

- **Go 1.27+** (`go version` ≥ go1.27). No other runtime dependencies for the broker itself; everything is the standard library plus grpc + protobuf (already vendored in `go.sum`).
- A POSIX shell for the quickstarts below. `make` is optional — every target is a thin wrapper around `go`.

## Build

```sh
git clone <repo-url> slipway
cd slipway
make build        # or: go build ./...
make verify       # go vet ./... && go test ./...
```

Two binaries result:

| Binary | Purpose |
|---|---|
| `cmd/slipwayd` | The broker daemon (HTTP admin + gRPC broker) |
| `cmd/slipway-cli` | `slipway-cli` — operational CLI (topics, produce/fetch, groups, DLQ, API keys) |

Install into `$GOBIN` (or `$GOPATH/bin`):

```sh
go install ./cmd/slipwayd ./cmd/slipway-cli
```

## Single-node quickstart (five minutes)

Run the broker:

```sh
go run ./cmd/slipwayd
# HTTP admin/health on :9092, gRPC on :9093; data in ./data ("" = in-memory)
```

In another terminal — no auth yet, so plain `curl` works (auth switches on once you create an API key; see below):

```sh
# Create a stream topic with 3 partitions
curl -X POST localhost:9092/topics -d '{"name":"orders","partitions":3}'

# Produce (FNV(key) partitions the record) via the CLI
slipway-cli produce --topic orders --key user-42 --value '{"order":123}'

# Fetch from a partition
slipway-cli fetch --topic orders --partition 0
```

Or drive everything over the HTTP admin API:

```sh
curl -X POST localhost:9092/topics -d '{"name":"orders","partitions":3}'
curl localhost:9092/topics
curl localhost:9092/healthz
curl localhost:9092/metrics
```

Checkpoint, full examples, and the API reference: [`docs/api.md`](api.md).

## Single-node with TLS and auth

TLS on both listeners and bearer-token auth are optional but recommended anywhere past a laptop.

```sh
# 1. cert + key (self-signed is fine for a first run; use a real CA in prod)
openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" -days 365

# 2. run with TLS
go run ./cmd/slipwayd -tls-cert cert.pem -tls-key key.pem

# 3. create the first (admin) key — plaintext is shown exactly once
curl --cacert cert.pem -X POST https://localhost:9092/apikeys \
  -H 'Content-Type: application/json' -d '{"name":"root","role":"admin"}'

# 4. use it
slipway-cli -ca-cert cert.pem -api-key 'ev-…' \
  produce --topic orders --key k --value v
```

Key details (all verified live):

- **Open by default until the first key exists.** Before you create one, every admin endpoint answers normally (this is the dev default). The moment one non-revoked key exists, all non-public routes (`/healthz` and `/metrics` stay public) require `Authorization: Bearer <key>`. There is no "off" switch other than revoking all keys.
- **Roles:** `viewer` (GET topics/groups) < `operator` (produce, queue pull/ack, topic create) < `admin` (API key management + everything operator can do).
- **CLI env vars:** `SLIPWAY_ADDR`, `SLIPWAY_HTTP_ADDR`, `SLIPWAY_PEERS`, `SLIPWAY_HTTP_PEERS`, `SLIPWAY_API_KEY`, `SLIPWAY_CA_CERT` — so `slipway-cli` works in scripts without repeating flags.
- Audit records for every admin mutation and refused request go to the replicated `__audit` topic. See [`docs/configuration.md`](configuration.md) and the `__audit` segment format in [`docs/api.md`](api.md).

## Three-node cluster (experimental, ADR-007)

```sh
# terminal 1
go run ./cmd/slipwayd -node-id n1 \
  -peers n1=127.0.0.1:9311,n2=127.0.0.1:9312,n3=127.0.0.1:9313 \
  -http-addr 127.0.0.1:9211 -grpc-addr 127.0.0.1:9311 -data-dir ./data-n1

# terminal 2
go run ./cmd/slipwayd -node-id n2 \
  -peers n1=127.0.0.1:9311,n2=127.0.0.1:9312,n3=127.0.0.1:9313 \
  -http-addr 127.0.0.1:9212 -grpc-addr 127.0.0.1:9312 -data-dir ./data-n2

# terminal 3 — same pattern
```

```sh
# client side: the CLI finds the leader/lease holder itself from metadata
slipway-cli -peers 127.0.0.1:9311,127.0.0.1:9312,127.0.0.1:9313 \
  -http-peers 127.0.0.1:9211,127.0.0.1:9212,127.0.0.1:9213 \
  create-topic --name orders --partitions 3
slipway-cli -peers 127.0.0.1:9311,127.0.0.1:9312,127.0.0.1:9313 \
  produce --topic orders --key k --value hello
```

Cluster notes and caveats (lease re-grant lands on the quorum leader; fixed membership; at-risk unsealed tail): see [`docs/adr/007-scaled-deployment.md`](adr/007-scaled-deployment.md). Internal topics (`__consumer_offsets`, `__queue_attempts`, `__audit`, `__api_keys`) replicate with the same block machinery since phase F.

## What gets persisted where (`-data-dir`, default `./data`)

| Path | Contents |
|---|---|
| `topics.json` | Topic registry (name, partitions, mode, created-at). Atomically renamed + fsynced. |
| `<topic>-<partition>/` | Segmented append-only log for one partition: `00000000000000000000.log`, `.index` sidecar per segment. CRC-framed records. |
| `__consumer_offsets-0/` | Committed consumer-group offsets (queue cursors ride the same log). |
| `__queue_attempts-0/` | Per-message queue attempt counters (redelivery + DLQ bookkeeping). |
| `__audit-0/` | Audit records (topic create/revoke, key create/revoke, authz refusals). |
| `__api_keys-0/` | Hashed API keys (SHA-256; plaintext never at rest). |
| `metadata/hardstate.json`, `metadata/log.bin` | Cluster-mode only: Raft hard state + quorum log (topic creates, block commits, lease grants). |

Every file under `-data-dir` may be copied for backup only with the broker stopped. There is no format version stamp yet (see [ADR-009](adr/009-storage-solutions.md)) — treat copies as opaque until one ships.

## Uninstall / cleanup

```sh
rm -rf ./data                      # delete all broker state (single node)
go clean -cache -testcache          # optional: wipe build/test caches
```

For cluster mode, wipe each node's `-data-dir` — they are independent file sets; the quorum recovers from a majority.
