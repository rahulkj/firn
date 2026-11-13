# Installing and running firn

Firn is a from-scratch event broker: **streaming** (topics, partitions, consumer groups, replay) and **queue** (ack, redelivery, DLQ) semantics over a shared append-only log. This page takes you from a clean checkout to a running broker with traffic flowing.

## Secure by default (read this before the first command)

**A fresh Firn broker serves nothing except `/healthz` until you mint the first API key.** On first start the daemon prints a one-time `BOOTSTRAP TOKEN` to stderr. Mint the first (admin) key with that token, then every other endpoint requires a bearer key forever after. See [migration-v0.3-to-v0.4.md](migration-v0.3-to-v0.4.md) if you are upgrading from v0.3.

## Prerequisites

- **Go 1.27+** (`go version` ≥ go1.27). No other runtime dependencies for the broker itself; everything is the standard library plus grpc + protobuf (already vendored in `go.sum`).
- A POSIX shell for the quickstarts below. `make` is optional — every target is a thin wrapper around `go`.

## Build

```sh
git clone <repo-url> firn
cd firn
make build        # or: go build ./...
make verify       # go vet ./... && go test ./...
```

Two binaries result:

| Binary | Purpose |
|---|---|
| `cmd/firnd` | The broker daemon (HTTP admin + gRPC broker) |
| `cmd/firn-cli` | `firn-cli` — operational CLI (topics, produce/fetch, groups, DLQ, API keys) |

Install into `$GOBIN` (or `$GOPATH/bin`):

```sh
go install ./cmd/firnd ./cmd/firn-cli
```

## Single-node quickstart (five minutes)

Run the broker:

```sh
go run ./cmd/firnd
# HTTP admin/health on :9092, gRPC on :9093; data in ./data ("" = in-memory)
# first-ever run prints "BOOTSTRAP TOKEN: <hex64>" on stderr — see next section
```

On first start the daemon prints a one-time `BOOTSTRAP TOKEN` to stderr (64 hex chars). Use it to mint the first (admin) key — exactly once. The token is then void:

```sh
# copy BOOTSTRAP TOKEN from the daemon stderr/log
TOKEN=<the 64-hex token you saw>

KEY=$(curl -s -X POST localhost:9092/apikeys \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"root","role":"admin"}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["key"])')
echo "$KEY" > ~/.firn-root-key
```

Then — everything needs the key:

```sh
# Create a stream topic with 3 partitions
curl -H "Authorization: Bearer $KEY" -X POST localhost:9092/topics -d '{"name":"orders","partitions":3}'

# Produce (FNV(key) partitions the record) via the CLI
firn-cli -api-key "$KEY" produce --topic orders --key user-42 --value '{"order":123}'

# Fetch from a partition
firn-cli -api-key "$KEY" fetch --topic orders --partition 0
```

Or drive everything over the HTTP admin API:

```sh
curl -H "Authorization: Bearer $KEY" -X POST localhost:9092/topics -d '{"name":"orders","partitions":3}'
curl -H "Authorization: Bearer $KEY" localhost:9092/topics
curl localhost:9092/healthz                                        # anonymous
curl -H "Authorization: Bearer $KEY" localhost:9092/metrics
```

Checkpoint, full examples, and the API reference: [`docs/api.md`](api.md).

## Single-node with TLS and auth

TLS on both listeners and bearer-token auth are optional but recommended anywhere past a laptop.

```sh
# 1. cert + key (self-signed is fine for a first run; use a real CA in prod)
openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" -days 365

# 2. run with TLS (BOOTSTRAP TOKEN is printed on stderr on first start)
go run ./cmd/firnd -tls-cert cert.pem -tls-key key.pem
TOKEN=$(...)   # copy from stderr output

# 3. create the first (admin) key — plaintext is shown exactly once
curl --cacert cert.pem -X POST https://localhost:9092/apikeys \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"name":"root","role":"admin"}'

# 4. use it
firn-cli -ca-cert cert.pem -api-key 'ev-…' \
  produce --topic orders --key k --value v
```

Key details (all verified live):

- **Fail-closed by default.** A fresh daemon with zero keys serves `/healthz` only; everything else returns 401/`Unauthenticated`. On first start it prints a one-time `BOOTSTRAP TOKEN` (64 hex chars) to stderr; `POST /apikeys` with it mints exactly one **admin** key (token void after use; role coerced to admin). After that, all non-public routes require a bearer key; `/healthz` stays anonymous; `/metrics` requires a key by default. `-dev` opts back into fully-open mode for local work (loud warning at startup).
- **Roles:** `viewer` (GET topics/groups) < `operator` (produce, queue pull/ack, topic create) < `admin` (API key management + everything operator can do).
- **CLI env vars:** `FIRN_ADDR`, `FIRN_HTTP_ADDR`, `FIRN_PEERS`, `FIRN_HTTP_PEERS`, `FIRN_API_KEY`, `FIRN_CA_CERT` — so `firn-cli` works in scripts without repeating flags.
- Audit records for every admin mutation and refused request go to the replicated `__audit` topic. See [`docs/configuration.md`](configuration.md) and the `__audit` segment format in [`docs/api.md`](api.md).

## Three-node cluster (experimental, ADR-007)

```sh
# 0. provision the mesh CA once for all three nodes
firn-cli mesh-ca init -out mesh-ca
for d in n1 n2 n3; do mkdir -p data-$d/mesh-ca && cp mesh-ca/ca.* data-$d/mesh-ca/ && chmod 0600 data-$d/mesh-ca/ca.*; done

# 1. terminal 1
go run ./cmd/firnd -node-id n1 \
  -peers n1=127.0.0.1:9311,n2=127.0.0.1:9312,n3=127.0.0.1:9313 \
  -http-addr 127.0.0.1:9211 -grpc-addr 127.0.0.1:9311 -data-dir ./data-n1

# terminal 2
go run ./cmd/firnd -node-id n2 \
  -peers n1=127.0.0.1:9311,n2=127.0.0.1:9312,n3=127.0.0.1:9313 \
  -http-addr 127.0.0.1:9212 -grpc-addr 127.0.0.1:9312 -data-dir ./data-n2

# terminal 3 — same pattern
```

```sh
# client side: the CLI finds the leader/lease holder itself from metadata
firn-cli -peers 127.0.0.1:9311,127.0.0.1:9312,127.0.0.1:9313 \
  -http-peers 127.0.0.1:9211,127.0.0.1:9212,127.0.0.1:9213 \
  create-topic --name orders --partitions 3
firn-cli -peers 127.0.0.1:9311,127.0.0.1:9312,127.0.0.1:9313 \
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
