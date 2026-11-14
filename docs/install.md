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
# HTTP admin/health on :9460, gRPC on :9461; data in ./data ("" = in-memory)
# first-ever run prints "BOOTSTRAP TOKEN: <hex64>" on stderr — see next section
```

On first start the daemon prints a one-time `BOOTSTRAP TOKEN` to stderr (64 hex chars). Use it to mint the first (admin) key — exactly once. The token is then void:

```sh
# copy BOOTSTRAP TOKEN from the daemon stderr/log
TOKEN=<the 64-hex token you saw>

KEY=$(curl -s -X POST localhost:9460/apikeys \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"root","role":"admin"}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["key"])')
echo "$KEY" > ~/.firn-root-key
```

Then — everything needs the key:

```sh
# Create a stream topic with 3 partitions
curl -H "Authorization: Bearer $KEY" -X POST localhost:9460/topics -d '{"name":"orders","partitions":3}'

# Produce (FNV(key) partitions the record) via the CLI
firn-cli -api-key "$KEY" produce --topic orders --key user-42 --value '{"order":123}'

# Fetch from a partition
firn-cli -api-key "$KEY" fetch --topic orders --partition 0
```

Or drive everything over the HTTP admin API:

```sh
curl -H "Authorization: Bearer $KEY" -X POST localhost:9460/topics -d '{"name":"orders","partitions":3}'
curl -H "Authorization: Bearer $KEY" localhost:9460/topics
curl localhost:9460/healthz                                        # anonymous
curl -H "Authorization: Bearer $KEY" localhost:9460/metrics
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
curl --cacert cert.pem -X POST https://localhost:9460/apikeys \
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

## Three-node cluster (v0.5.0+, GA — ADR-007)

```sh
# 0. provision the mesh CA once for all three nodes
firn-cli mesh-ca init -out mesh-ca
for d in n1 n2 n3; do mkdir -p data-$d/mesh-ca && cp mesh-ca/ca.* data-$d/mesh-ca/ && chmod 0600 data-$d/mesh-ca/ca.*; done

# 1. terminal 1
go run ./cmd/firnd -node-id n1 \
  -peers n1=127.0.0.1:9538,n2=127.0.0.1:9540,n3=127.0.0.1:9542 \
  -http-addr 127.0.0.1:9537 -grpc-addr 127.0.0.1:9538 -data-dir ./data-n1

# terminal 2
go run ./cmd/firnd -node-id n2 \
  -peers n1=127.0.0.1:9538,n2=127.0.0.1:9540,n3=127.0.0.1:9542 \
  -http-addr 127.0.0.1:9539 -grpc-addr 127.0.0.1:9540 -data-dir ./data-n2

# terminal 3 — same pattern
```

```sh
# client side: the CLI finds the leader/lease holder itself from metadata
firn-cli -peers 127.0.0.1:9538,127.0.0.1:9540,127.0.0.1:9542 \
  -http-peers 127.0.0.1:9537,127.0.0.1:9539,127.0.0.1:9541 \
  create-topic --name orders --partitions 3
firn-cli -peers 127.0.0.1:9538,127.0.0.1:9540,127.0.0.1:9542 \
  produce --topic orders --key k --value hello
```

Cluster notes and caveats (lease re-grant lands on the quorum leader; fixed membership; at-risk unsealed tail): see [`docs/adr/007-scaled-deployment.md`](adr/007-scaled-deployment.md). Internal topics (`__consumer_offsets`, `__queue_attempts`, `__audit`, `__api_keys`) replicate with the same block machinery since phase F.

## Two-cluster federation (preview, ADR-010)

Federation lets two Firn clusters replicate topics (and queues) to each other over the mesh listener. Mode is per peer: `passive` (one-way source → sink, sink fenced until cutover) or `active-active` (both clusters accept writes; records are re-originated with fresh local offsets). Federation is pull-only in v1 — no push. Both clusters must share the mesh CA (or the sink can pin the source's CA via the peer's `ca_crt_pem`).

```sh
# 0. one shared mesh CA for the pair (or use per-cluster CAs with ca_crt_pem pinning)
firn-cli mesh-ca init -out shared-ca
for d in east west; do mkdir -p $d/mesh-ca && cp shared-ca/ca.* $d/mesh-ca/ && chmod 0600 $d/mesh-ca/ca.*; done

# 1. boot both — -federation-self is REQUIRED for active-active (echo filter)
firnd -data-dir ./east -federation-self east -http-addr :9300 -grpc-addr :9301 -mesh-addr :9302 &
firnd -data-dir ./west -federation-self west -http-addr :9410 -grpc-addr :9411 -mesh-addr :9412 &

# 2. mint one admin key per cluster via the bootstrap token (printed on stderr)
curl -s -X POST localhost:9300/apikeys -H "Authorization: Bearer <EAST_TOKEN>" -d '{"name":"root","role":"admin"}'
curl -s -X POST localhost:9410/apikeys -H "Authorization: Bearer <WEST_TOKEN>" -d '{"name":"root","role":"admin"}'

# 3. each side mints a FEDERATION key for the other
curl -s -X POST localhost:9300/federation/keys -H "Authorization: Bearer <EAST_ADMIN>" -d '{"name":"west"}'
curl -s -X POST localhost:9410/federation/keys -H "Authorization: Bearer <WEST_ADMIN>" -d '{"name":"east"}'

# 4. create the SAME topic on both sides (partition counts must match or the import refuses)
curl -s -X POST localhost:9300/topics -H "Authorization: Bearer <EAST_ADMIN>" -d '{"name":"orders","partitions":1}'
curl -s -X POST localhost:9410/topics -H "Authorization: Bearer <WEST_ADMIN>" -d '{"name":"orders","partitions":1}'

# 5. register peers both directions (active-active)
firn-cli -http-addr localhost:9300 -api-key <EAST_ADMIN> federation add-peer --name west --addr 127.0.0.1:9412 --server-name <west-node-id> --direction both --mode active-active --topics orders --key <KEY_WEST_ISSUED>
firn-cli -http-addr localhost:9410 -api-key <WEST_ADMIN> federation add-peer --name east --addr 127.0.0.1:9302 --server-name <east-node-id> --direction both --mode active-active --topics orders --key <KEY_EAST_ISSUED>

# 6. write on either side; both clusters converge
firn-cli -addr localhost:9301 -api-key <EAST_ADMIN> produce --topic orders --key k1 --value hello
firn-cli -addr localhost:9411 -api-key <WEST_ADMIN> fetch --topic orders --offset 0
```

For **passive (one-way DR)**: boot the sink with the same CA and `-federation-self`, create the topic on both sides, register the sink with `--direction in --mode passive`, and produce only on the source — the sink is fenced (produce/pull/ack → `409`/`FailedPrecondition`) until the operator cuts over (see `docs/api.md`).

`firn federation status` shows per-`(peer, topic, partition)` import cursors; offsets are preserved in passive imports and re-originated to fresh local offsets in active-active imports (ts/key/value preserved either way). For cutting a passive sink over to writable: [`docs/federation-cutover.md`](federation-cutover.md). `firn_federation_cursor_lag_seconds{peer,topic,partition}` reports lag; `/ui/federation` is read-only. Constraints: push federation doesn't exist, cross-cluster ordering is undefined, DLQ trims stay at-least-once across the pair — see [`docs/adr/010-federation.md`](adr/010-federation.md) and [`docs/plans/004-federation.md`](plans/004-federation.md).

## What gets persisted where (`-data-dir`, default `./data`)

| Path | Contents |
|---|---|
| `topics.json` | Topic registry (name, partitions, mode, created-at). Atomically renamed + fsynced. |
| `<topic>-<partition>/` | Segmented append-only log for one partition: `00000000000000000000.log`. CRC-framed records; sparse index in memory, rebuilt by scanning on open (no persisted `.index` files yet). |
| `__consumer_offsets-0/` | Committed consumer-group offsets (queue cursors ride the same log). |
| `__queue_attempts-0/` | Per-message queue attempt counters (redelivery + DLQ bookkeeping). |
| `__audit-0/` | Audit records (topic create/revoke, key create/revoke, authz refusals). |
| `__api_keys-0/` | Hashed API keys (SHA-256; plaintext never at rest). |
| `__federation_peers-0/` | Federation peer registry (addr, key ref, policy). Replicates intra-cluster; never federates. |
| `__federation_cursors-0/` | Per-peer import cursors + foreign-applied ranges (echo filter). Replicates intra-cluster; never federates. |
| `metadata/hardstate.json`, `metadata/log.bin` | Cluster-mode only: Raft hard state + quorum log (topic creates, block commits, lease grants). |

Every file under `-data-dir` may be copied for backup only with the broker stopped. There is no format version stamp yet (see [ADR-009](adr/009-storage-solutions.md)) — treat copies as opaque until one ships.

## Uninstall / cleanup

```sh
rm -rf ./data                      # delete all broker state (single node)
go clean -cache -testcache          # optional: wipe build/test caches
```

For cluster mode, wipe each node's `-data-dir` — they are independent file sets; the quorum recovers from a majority.
