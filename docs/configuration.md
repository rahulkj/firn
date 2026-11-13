# Configuration reference

Two binaries, every flag, the defaults that ship, and what they do. Values below were taken from the source at this commit — if a flag here disagrees with `-h` output, the source wins and the doc is a bug; file it.

## `firnd` — broker daemon

Defaults (in-memory except `./data`):

```sh
firnd \
  -http-addr :9092 \
  -grpc-addr :9093 \
  -data-dir ./data \
  -session-timeout 10s \
  -retention-interval 1m
  # plus optional: -segment-bytes, -block-seal-interval, -retention-ttl, -retention-max-bytes
```

| Flag | Default | Type | Effect |
|---|---|---|---|
| `-http-addr` | `:9092` | host:port | HTTP admin/health listener (`/healthz`, `/metrics`, admin API, `/ui/`). Auth enforced on the admin mux once any key exists. |
| `-grpc-addr` | `:9093` | host:port | gRPC listener: `FirnService` + (cluster) `ReplicaService`/`QuorumService`. This is the port clients dial. |
| `-data-dir` | `./data` | path or `""` | Partition logs + internal topics + `topics.json` + (cluster) quorum state. `""` selects in-memory `MemLog` — dev/tests only; **restart loses everything**, no durability. |
| `-retention-ttl` | `0` (disabled) | duration | Delete **whole segments** whose newest record is older than this. Active (newest) segment is never deleted. `0` disables. |
| `-retention-max-bytes` | `0` (disabled) | bytes | Max on-disk bytes per partition log; oldest whole segments deleted first. Active segment never deleted. |
| `-retention-interval` | `1m` | duration | How often the retention sweeper runs. Only active when TTL or max-bytes is set. |
| `-segment-bytes` | `0` (= broker default 64 MiB) | bytes | Max on-disk bytes per **segment** per partition log. Segments roll at the cap; smaller caps are what make whole-segment retention actually deletable. |
| `-block-seal-interval` | `0` | duration | Roll (seal) a partition's active segment once it has outlived this age, even with no size pressure (applied on the next append / replication tick). `0` = seal on size only. |
| `-session-timeout` | `10s` | duration | Consumer-group member liveness lease. Members that miss this window are evicted and re-balance. |
| `-node-id` | `""` | string | Cluster mode: this node's ID. Empty = single-node (no quorum, no replication). |
| `-peers` | `""` | `id=addr,...` | Cluster mode, **including self**: `n1=127.0.0.1:9311,n2=127.0.0.1:9312,n3=127.0.0.1:9313`. Must list every node; must include this node's `-node-id`. |
| `-lease-ttl` | `3s` | duration | Cluster mode: write-lease TTL per partition. Leases expire (crashed holder loses the slot); the quorum re-grants. |
| `-tls-cert` | `""` | PEM path | TLS certificate for **both** listeners. Empty (with `-tls-key`) = plaintext. Both must be set together. |
| `-tls-key` | `""` | PEM path | TLS private key for both listeners. Minimum version TLS 1.2; single cert for both, so give SANs covering both hostnames/IPs. |
| `-metrics-auth` | `true` | bool | Require a viewer key on `/metrics` (set false for Prometheus scrape without credentials; not recommended). |
| `-anon-rate` | `20` | int | Max anonymous operations per `-anon-window` per source IP (covers `/healthz` probes and all unauthenticated refusals). `0` = unlimited. |
| `-anon-window` | `1m` | duration | Window for `-anon-rate`. |
| `-rate` | `100` | float ops/s | Per-key token-bucket rate limit, enforced at the HTTP and gRPC interceptors. Applies once auth is enforced (any key exists); limits are per key, not per IP. |
| `-admin-ui` | `true` | bool | Serve the read-only admin UI at `/ui/`. Cheap; safe to leave on behind TLS+auth. |
| `-mesh-addr` | derived | host:port | Mesh (internal) gRPC listener address — defaults to `-grpc-addr` port+10 (crash if colliding). Serves only `ReplicaService`/`QuorumService` behind mesh mTLS. |
| `-mesh-ca-bundle` | `""` | PEM path | Pre-provisioned mesh cluster CA certificate PEM (paired with `-mesh-ca-key`). REQUIRED for non-dev, non-single-node clusters. |
| `-mesh-ca-key` | `""` | PEM path | Pre-provisioned mesh cluster CA private key PEM. |

Startup sequence (single node): open registry → open every partition log + internal topics (`__consumer_offsets`, `__queue_attempts`, `__audit`, `__api_keys`) → fold committed offsets → serve. Crash recovery order per log: scan segments, CRC-validate frames, truncate any torn tail (`internal/log/filelog.go`), then mark blocks sealed. Cluster adds: start Raft node → wait to observe a leader (or elect itself) → replay `topics.json` state via apply hook → replication agent starts pushing/pulling blocks + granting leases.

Cluster rules: membership is **fixed at startup** (no join/leave); topics created in cluster mode go through the quorum leader and return HTTP `202 propagating`; produce/fetch are routed by the client from `GetMetadata`.

Cluster rules for internal topics: each internal topic (`__consumer_offsets`, `__queue_attempts`, `__audit`, `__api_keys`) has exactly one **write lease** at a time — the same lease table that fences user-topic appends. Only the lease holder may write; every other node fails closed (HTTP 503 / gRPC `FailedPrecondition` on its write paths). Reads are served on every node. In single-node mode all writes are allowed. There is no `-internal-lease-ttl` knob — the fencing rides `-lease-ttl`.

### Startup modes: fail-closed, bootstrap, and enforced (security model)

The broker is **fail-closed by default**. Three modes exist:

1. **Enforced** (default once keys exist): every request except `/healthz` requires `Authorization: Bearer <key>` with a valid role.
2. **Bootstrap** (zero keys, no escape flag): everything except `/healthz` returns `401`; the broker printed a one-time `BOOTSTRAP TOKEN` to stderr on first start. `POST /apikeys` with that bearer mints exactly one **admin** key; the token is then void; enforced mode is active from the next request. Restart with existing keys enforces immediately — no token is reprinted.
3. **Open** (`-dev` or `-allow-open-bootstrap`): no keys required anywhere, including `/metrics` and produce/fetch. Loud warning on startup. Development only.

In bootstrap mode gRPC returns `Unauthenticated` for every call; minting happens over HTTP. Client authenticated with the minted admin key may then use all endpoints and future restarts enforce with zero operator action.

## `firn-cli` — operational CLI

Defaults `localhost:9093` / `localhost:9092`, overridable per flag or env.

| Flag | Env var | Default | Effect |
|---|---|---|---|
| `-addr` | `FIRN_ADDR` | `localhost:9093` | Seed gRPC address. In cluster mode the CLI still asks this seed for metadata first, then routes to the lease holder for the partition (2s metadata cache, refresh-on-retry). |
| `-http-addr` | `FIRN_HTTP_ADDR` | `localhost:9092` | Seed HTTP admin address for topic creation, API keys, groups. |
| `-peers` | `FIRN_PEERS` | `""` | Cluster mode: comma-separated node gRPC addresses (`host:port,host:port,…`). CLI picks seeds round-robin when metadata is unavailable. |
| `-http-peers` | `FIRN_HTTP_PEERS` | derived | Cluster mode: comma-separated HTTP admin addresses. Defaults to `-peers` with each port decremented by 1. |
| `-json` | — | `false` | Machine-readable output for list/produce/groups/dlq/apikeys commands. |
| `-api-key` | `FIRN_API_KEY` | `""` | Bearer key. Required whenever the broker enforces auth. |
| `-ca-cert` | `FIRN_CA_CERT` | `""` | PEM CA bundle. When set: gRPC dials with TLS (CA pool) and the HTTP admin client switches to HTTPS. Self-signed server certs are fine if the SAN covers dial host/IP. |

Command surface (see `firn-cli -h`):

```
create-topic --name N [--partitions 2] [--mode stream|queue]
create-queue --name N [--partitions 2]
list
produce --topic T --key K --value V [--offset-debug]
fetch --topic T --partition P [--offset N] [--max N]
groups
dlq list --queue Q | dlq redrive --queue Q
create-apikey --name N [--role viewer|operator|admin]
revoke-apikey --name N
list-apikeys
```

`dlq redrive` works entirely over the gRPC path: it fetches `<queue>.dlq` and re-produces into `<queue>` — no HTTP admin route is involved.

## Retention horizon rules (all modes)

1. Deletion never touches the **active** (newest, writable) segment of a partition log.
2. Whole segments only — retention never rewrites records or renumbers offsets.
3. Offsets below the retention horizon return `FailedPrecondition` on fetch (this is deliberate: silent holes would corrupt consumer-group bookkeeping).
4. `-retention-ttl` / `-retention-max-bytes` are independent; either triggers the sweep; both are per-partition.
5. Retention is exercisable end-to-end (both TTL and max-bytes paths verified live). The `-segment-bytes` flag makes segments roll at the cap (default 64 MiB); `-block-seal-interval` additionally rolls the active segment once it outlives the configured age. Recipe: run a fresh node with `-segment-bytes 2000 -retention-max-bytes 1200 -retention-interval 500ms` (or `-retention-ttl 2s`), produce a few hundred records, and watch `deleted_segments` in the log; `fetch --offset 0` then returns `FailedPrecondition: offset below retention horizon`. See [ADR-009](adr/009-storage-solutions.md).

## Data-directory format

Not versioned. Any layout change (frame header, `topics.json` schema) is a breaking change until a format version byte ships. Backups: `cp -a` the whole `-data-dir` **with the broker stopped**; never patch individual segments by hand.

## Logging

JSON via `log/slog` on stderr (`firnd`); the CLI prints human tables by default and JSON with `-json`. Log level is not a flag — it follows `slog.SetDefault` (`info` default, debug via code); wire your own if you embed `cmd/firnd` as a library.
