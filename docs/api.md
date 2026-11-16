# API reference

Firn exposes two surfaces:

| Surface | Port (default) | Protocol | Audience |
|---|---|---|---|
| Admin / operations | `:9460` | HTTP + JSON | Operators, CLI, health checks |
| Broker / data plane | `:9461` | gRPC (HTTP/2, protobuf) | Applications, `firn-cli` |

Plus three **internal** services on the gRPC port used between nodes — documented here for completeness, not for client use: `QuorumService` (Raft messages), `ReplicaService` (block push/pull), and the gRPC path of `GetMetadata`.

Everything in this document was cross-checked against `internal/api/firn.proto` and the handlers; request/response samples marked *(live)* were copied from a running broker.

## Authentication & authorization (since v0.1.0 + phase E)

- **Fail-closed by default.** With zero keys and no `-dev`/`-allow-open-bootstrap` flag, the broker serves `/healthz` only; every other endpoint returns 401 (gRPC: `Unauthenticated`). On first start it prints a one-time `BOOTSTRAP TOKEN` to stderr.
- **Bootstrap**: `POST /apikeys` with `Authorization: Bearer <BOOTSTRAP TOKEN>` mints exactly one **admin** key. The token is then void. Role in the body is ignored (forced `admin`). Only reachable until that call succeeds.
- **Enforced**: once one non-revoked key exists, every non-public route requires `Authorization: Bearer <key>`.
  - HTTP: header on every request. CLI: `-api-key` flag or `FIRN_API_KEY` env var.
  - gRPC: per-RPC `authorization` metadata (CLI attaches automatically when `-api-key` is set).
- `/healthz` is public. `/metrics` requires a `viewer` key by default (`-metrics-auth=false` restores unauth scraping).
- Roles (least → most privilege): `viewer` → `operator` → `admin`.
  - `viewer`: `GET` topics/groups/apikeys metadata (apikeys list itself is admin).
  - `operator`: produce/fetch/queue pull+ack/commit-offset/topic create/group ops.
  - `admin`: everything + key create/revoke.
- Per-key token-bucket rate limit, default 100 ops/s (`-rate` flag); excess → HTTP `429` / gRPC `RESOURCE_EXHAUSTED`.
- Anonymous traffic is capped per IP (`-anon-rate` / `-anon-window`, default 20 ops per minute); refusals audit-recorded in every mode.
- Keys may carry an `allowed_ips` CIDR allowlist; mismatched callers get HTTP `403` / gRPC `PERMISSION_DENIED` with `authz-refused:ip-not-allowed` in `__audit`.
- *(Cluster mode)* Internal-topic writes (`__consumer_offsets` commits, `__queue_attempts`, `__audit`, `__api_keys`) are fenced by the internal lease: only the holder may write. Other nodes fail closed with **HTTP 503 / gRPC `FailedPrecondition`** containing the marker `fenced` plus the holder. Apps must retry (the CLI's `create-apikey`/`revoke-apikey` already retry across seed nodes on 503). Reads are open on every node.
- Error bodies on refusal: `{"error":"<reason>"}` with HTTP `401` (missing/unknown/revoked key), `403` (insufficient role or IP not in key allowlist), `429` (rate limited or anonymous rate limit), `503` (fenced). *(live — see verification logs)*

---

## HTTP admin surface (`:9460`)

### `GET /healthz`

```sh
curl localhost:9460/healthz
```
```json
{"status":"ok","version":"0.5.5"}
```

### `GET /metrics`

Prometheus text exposition format (ADR-006). Rate-limited counters, group/queue depth gauges, partition watermarks. `/metrics` requires a viewer key by default (`-metrics-auth=true`; see `docs/configuration.md` / `docs/adr/011-secure-mode.md`); set `-metrics-auth=false` only if scraping from a private network.

### `POST /topics`

Create a topic. Stream or queue.

```sh
curl -X POST localhost:9460/topics -d '{"name":"orders","partitions":3,"mode":"stream"}'
```
```json
{"name":"orders","partitions":3,"mode":"stream"}
```
HTTP `201 Created`. In cluster mode the response is `202 Accepted` with `{"status":"propagating", ...}` — the topic lands via the quorum and followers converge within a replication tick (~hundreds of ms).

Errors: `400` invalid partitions/mode · `409` topic exists · `500` internal · `401/403` auth.

### `GET /topics`

```sh
curl localhost:9460/topics
```
```json
{"topics":[{"name":"orders","partitions":3,"mode":"stream"}]}
```

### `GET /groups`

```sh
curl -H "Authorization: Bearer $KEY" localhost:9460/groups
```
```json
{
  "groups": [
    {
      "name": "checkout",
      "generation": 3,
      "leader": "m-9q3x7",
      "protocol": "range",
      "members": [
        {
          "id": "m-9q3x7",
          "topics": ["orders"],
          "assignment": {"orders": [0, 1]},
          "lastHeartbeat": 1794579410000
        }
      ]
    }
  ]
}
```

### `GET /groups/{name}/offsets`

```sh
curl -H "Authorization: Bearer $KEY" localhost:9460/groups/checkout/offsets
```
```json
{
  "group": "checkout",
  "offsets": [
    {"group":"checkout","topic":"orders","partition":0,"offset":42,"generationId":3,"leader":"m-9q3x7"}
  ]
}
```

### `POST /queues/{name}/pull`

Claim up to N messages for `visibility_timeout_ms` (default 30 000, hard cap 500 messages).

```sh
curl -X POST localhost:9460/queues/jobs/pull \
  -d '{"max_messages":10,"visibility_timeout_ms":30000}'
```
```json
{
  "deliveries": [
    {
      "deliveryId": "d-3f2a9c…",
      "queue": "jobs",
      "partition": 0,
      "offset": 7,
      "key": "order",
      "value": "{\"order\":123}",
      "attempts": 1,
      "deadlineMs": 1794579440000
    }
  ]
}
```
Errors: `404` queue not found / unknown delivery · `400` not a queue topic · `401/403` auth.

### `POST /queues/{name}/ack`

```sh
curl -X POST localhost:9460/queues/jobs/ack -d '{"delivery_id":"d-3f2a9c…"}'
```
```json
{"status":"ok"}
```

### `POST /apikeys` *(admin)*

Plaintext key is returned **exactly once**; it is never stored and cannot be recovered.

```sh
curl -X POST localhost:9460/apikeys \
  -H 'Authorization: Bearer $ADMIN_KEY' -H 'Content-Type: application/json' \
  -d '{"name":"ci-deploy","role":"operator"}'
```
*(live)*
```json
{"key":"ev-IkwA1m2…","name":"ci-deploy","role":"operator"}
```

### `GET /apikeys` *(admin)*

*(live)*
```json
{
  "keys": [
    {"createdAt":1794579409194,"name":"root","revoked":false,"role":"admin"},
    {"createdAt":1794579409235,"name":"worker","revoked":false,"role":"operator"},
    {"createdAt":1794579409249,"name":"reader","revoked":false,"role":"viewer"}
  ]
}
```

### `DELETE /apikeys/{name}` *(admin)*

```sh
curl -X DELETE localhost:9460/apikeys/ci-deploy -H 'Authorization: Bearer $ADMIN_KEY'
```
```json
{"status":"revoked","name":"ci-deploy"}
```

### `GET /ui/`

Read-only HTML dashboard (topics, groups, generation/leader/members). Renders behind the same auth/401 as the rest of the admin surface. Browsers can't send a bearer header, so `/ui/*` also accepts the session cookie minted by `POST /ui/session` — see below. Meant for browsers on a trusted network.

### `POST /ui/session`

Mint a browser session for `/ui/*`. Requires `Authorization: Bearer <key>` (a `viewer` key is enough) — or a valid existing session cookie, in which case this issues a fresh token (sliding renewal). Federation-scoped keys are refused (`403`).

```sh
curl -c jar -X POST localhost:9460/ui/session -H "Authorization: Bearer $KEY"
# {"status":"session created","expires_unix":1794660094}
```

`201 Created` · `Set-Cookie: firn_ui_session=<token>; Path=/ui/; HttpOnly; SameSite=Strict; Max-Age=43200`. `Secure` is added when the request arrives over TLS or `X-Forwarded-Proto: https`.

Properties:
- The cookie is scoped `Path=/ui/` — it is **never** sent to the admin JSON API (`/topics`, `/groups`, … all stay bearer-only).
- Sessions are in-memory only: a restart signs every browser out. TTL 12h.
- Every cookie-authenticated request re-verifies the backing key, so **revoking a key invalidates its sessions immediately**.
- The per-key rate limit and `allowed_ips` allowlist apply to cookie auth exactly as to bearer auth.
- Grant/revoke are audit-recorded (`ui-session-granted` / `ui-session-revoked`).
- Each `POST` mints a fresh token; previously issued tokens remain valid until revoked or expired.

Errors: `401` missing/unknown key (or bootstrap mode) · `403` federation-scoped key · `400` auth disabled (`-dev`) · `429` rate limited.

### `DELETE /ui/session`

Revoke the session named by the cookie (idempotent — no cookie or an unknown session still returns `200`). Clears the cookie.

```sh
curl -X DELETE -b jar localhost:9460/ui/session
# {"status":"revoked"}
```

`200 OK`. Revocation is audit-recorded.

---

## gRPC FirnService (`:9461`)

Package `firn.v1` — full definitions in `internal/api/firn.proto`. All RPCs are unary; streaming is deliberately not in v1.

### Produce

```proto
message ProduceRequest  { string topic = 1; bytes key = 2; bytes value = 3; }
message ProduceResponse { int32 partition = 1; int64 offset = 2; }
```

Key set → FNV-64a(key) % partitions. Key unset → round-robin. Cluster mode: the CLI routes to the lease holder for the partition (from `GetMetadata`), retrying with a fresh metadata fetch on stale-hint errors.

*(live)*
```sh
firn-cli -api-key "$KEY" produce --topic orders --key k1 --value hello
# produced to orders partition=1 offset=0
```

### ProduceBatch (since Unreleased)

```proto
message ProduceBatchRecord    { bytes key = 1; bytes value = 2; }
message ProduceBatchRequest   { string topic = 1; repeated ProduceBatchRecord records = 2; }
message ProduceBatchRecordResult { int32 partition = 1; int64 offset = 2; }
message ProduceBatchResponse  { repeated ProduceBatchRecordResult records = 1; }
```

One RPC instead of N `Produce` calls; hard cap 500 records per call. Response order matches request order; partition assignment per record uses the same rule as `Produce` (FNV(key) % partitions, round-robin when unset).

Failure semantics: **not atomic**. Records are produced in order; a failure mid-batch stops the call with an error — records before it are durable, records after are not. At-least-once: retrying the whole batch re-produces it, so earlier records may appear at **new offsets** (duplicates). Consumers must already tolerate duplicates.

CLI: `firn-cli produce --topic orders --batch-file records.jsonl` (one JSON object per line: `{"key": "...", "value": "..."}`; `key` optional, omitted for round-robin; the `-` filename reads stdin; batches larger than 500 lines are chunked automatically).

Errors: `INVALID_ARGUMENT` (empty topic, empty batch, >500 records) · `NOT_FOUND` (topic doesn't exist) · `PERMISSION_DENIED`/`UNAUTHENTICATED` as usual.

### Fetch (federation watermark hint)

`FetchResponse.federation_hwm` (since v0.4.0): advisory minimum — the lowest `LastLocalOffset` applied from any inbound federation peer (passive or active-active) covering (topic, partition); `0` when unknown or not federated. Hint only; never a correctness input cross-cluster. `firn-cli fetch --json` includes it.

### Fetch

```proto
message FetchRequest  { string topic = 1; int32 partition = 2; int64 offset = 3; int32 max_records = 4; }
message FetchResponse { repeated Record records = 1; }
message Record        { int64 offset = 1; bytes key = 2; bytes value = 3; }
```

`max_records >= 1` (hard cap 1000). Fetching at offset ≥ high watermark returns zero records (not an error). Offsets below the retention horizon fail with `FailedPrecondition`.

### Consumer groups

`JoinGroup` → `SyncGroup` → (`Heartbeat` loop) → `LeaveGroup`. `protocol`: `range` (default) | `roundrobin`. `session_timeout_ms`: 0 = server default (10s). `CommitOffset` monotonicity enforced unless `allow_reset=true` (invariant 4); non-zero stale `generation_id` is rejected. Offsets are persisted to `__consumer_offsets` and survive restart.

```proto
message CommitOffsetRequest {
  string group_id = 1; string topic = 2; int32 partition = 3;
  int64 offset = 4; int64 generation_id = 5; bool allow_reset = 6;
}
message FetchOffsetRequest  { string group_id = 1; string topic = 2; int32 partition = 3; }
message FetchOffsetResponse { int64 offset = 1; bool found = 2; int64 generation_id = 3; string leader = 4; }
message FetchGroupOffsetsRequest { string group_id = 1; }
```

### Queue mode (ack / visibility / DLQ)

`Pull(queue, max_messages, visibility_timeout_ms)` → lease delivery. `Ack(queue, delivery_id)` settles it. Unacked messages are redelivered at lease expiry (at-least-once); after **3 delivery attempts** a message is diverted to `<queue>.dlq` (auto-created stream topic). DLQ recovery is manual: `firn-cli dlq list|redrive --queue <name>` — redrive re-produces records into the source queue.

**AckBatch** (since Unreleased): settle up to **500** deliveries in one RPC — cheaper for high-volume consumers (one `__consumer_offsets` commit per contiguous advance, not one per Ack). Unknown/already-settled ids are tolerated and **counted**, never failed — retries after a crash are safe. Response reports `acked` and `unknown`.

```proto
message AckBatchRequest  { string queue = 1; repeated string delivery_ids = 2; } // 1..500 ids
message AckBatchResponse { int32 acked = 1; int32 unknown = 2; }
```

Errors: `INVALID_ARGUMENT` (empty/oversized batch, wrong mode) · `NOT_FOUND` (queue) · `FAILED_PRECONDITION` (passively federated) · same role gates as `Ack`.

```proto
message QueueDelivery {
  string delivery_id = 1; string queue = 2; int32 partition = 3; int64 offset = 4;
  bytes key = 5; bytes value = 6; int32 attempts = 7; int64 deadline_ms = 8;
}
```

### GetMetadata

The routing primitive for cluster clients.

```proto
message GetMetadataRequest  { repeated string topics = 1; } // empty = all topics
message GetMetadataResponse {
  repeated BrokerMetadata brokers = 1;  // id, advertised gRPC addr, is_leader (quorum)
  repeated TopicMetadata topics   = 2;  // name, mode, partitions + per-partition {partition_id, leader, leader_addr}
}
```

- Single-node: one broker entry, `id: "single"`, `addr` = the node's `-grpc-addr` (wildcards folded to `localhost`), `leader: ""`/`leader_addr: ""` in `PartitionMetadata`. *(fixed in this pass — previously advertised a hardcoded `localhost:9461`; now reflects `-grpc-addr`.)*
- Cluster: one entry per `-peers` member with its advertised gRPC address; `PartitionMetadata.leader_addr` is the lease holder's dial address.
- CLI caches this for 2s; stale entries self-heal via retry-with-refresh.

---

## gRPC status codes (broker surface)

| Code | When |
|---|---|
| `OK` | success |
| `INVALID_ARGUMENT` | `max_records < 1`, invalid topic/mode/protocol, empty key fields |
| `NOT_FOUND` | topic/queue/group/delivery/member not found |
| `FAILED_PRECONDITION` | fetch below retention horizon; write on non-lease-holder (cluster); stale generation; queue topic in stream mode and vice versa; fenced internal topic (cluster mode — retry on the holder) |
| `UNAUTHENTICATED` | missing/unknown/revoked API key |
| `PERMISSION_DENIED` | valid key, insufficient role |
| `RESOURCE_EXHAUSTED` | per-key rate limit exceeded |
| `UNAVAILABLE` | broker down / connection refused (CLI retries with metadata refresh) |

## HTTP status codes (admin surface)

| Code | Meaning |
|---|---|
| `200` | OK (GET, pull, ack, revoked) |
| `201` | Created (topic, apikey) |
| `202` | Accepted — cluster topic create, propagating via quorum |
| `400` | Malformed JSON, bad arguments, not-a-queue |
| `401` | Missing/unknown/revoked key (`{"error":"missing bearer token"}` etc.) |
| `403` | Insufficient role |
| `404` | Not found |
| `405` | Wrong method for path |
| `409` | Topic already exists |
| `409` | Resource is passively federated on this cluster (`federated: passive`) — sink is read-only until cutover |
| `429` | Rate limited |
| `500` | Internal error |
| `503` | Subsystem not configured (e.g. queue manager on a read-only node); internal topic fenced on this node — another node holds the lease; retry there |

---

## Federation (preview, ADR-010)

Multiple Firn clusters can register as peers and replicate records one-way (`passive` — source → sink, byte-identical, offsets preserved) with the sink fenced against local writes until an operator cuts over. Registration is out-of-band: each side mints a federation-scoped key (`firn-cli federation keys`) and hands it to the other side.

| Method | Path | Description |
|---|---|---|
| `POST` | `/federation/peers` | Register (or overwrite) a peer. Body: `{name, addr, direction (in\|out\|both), mode (passive\|active-active), key, topics?[], queues?[], ca_crt_pem? (base64 PEM), server_name?}`. `key` is the bearer issued by the **peer**; `direction` `in`/`both` require it. Never echoed by any endpoint. Active-active requires `-federation-self` be set on both clusters. |
| `GET` | `/federation/peers` | List peers (`keySet` is reported; the key is not). |
| `DELETE` | `/federation/peers/{name}` | Remove a peer. Cursors for the peer are **not** deleted; a full re-import requires deleting the peer and re-registering (operator action). |
| `POST` | `/federation/keys` | Mint a federation-scoped key for *this* cluster (shown once). Body: `{name, allowed_ips?[]}`. |
| `GET` | `/federation/cursors` | Import progress per `(peer, topic, partition)`. |

gRPC: `FederationService` is served **only on the mesh listener** (mTLS + federation key). `FederationFetch` returns raw, CRC-checked frame windows for `[first_offset, last_offset]` (clamped to source HWM; internal topics refused except `__queue_attempts`). Normal keys cannot call it; federation keys cannot call anything else.

`firn-cli`: `firn-cli federation add-peer|list-peers|remove-peer|keys|status` — see the command map below.

Status codes: passive registration `201`; produce/pull/ack on a passively-federated resource → gRPC `FailedPrecondition` / HTTP `409` (`federated: passive`); sink stays read-only until cutover. Active-active mode is available (preview): both clusters accept writes, records are re-originated with fresh local offsets, ts/key/value preserved, and the A→B→A echo is filtered server-side via each node's foreign-applied local ranges. Full details: `docs/plans/004-federation.md`; cutover: `docs/federation-cutover.md`.

## Internal services (never call from clients)

`ReplicaService` and `QuorumService` are served **only** on the mesh gRPC listener behind mesh mTLS — they are **not** reachable from the public gRPC listener and are never advertised on the public port. Applications and admin tooling must never call them; they may change shape without deprecation notice. Access control: a caller without a node certificate signed by the cluster CA cannot complete the TLS handshake.

### ReplicaService

```proto
message ReplicaFetchRequest  { string topic = 1; int32 partition = 2; int64 first_offset = 3; int64 last_offset = 4; }
message ReplicaFetchResponse { bytes frames = 1; }
message ReplicaPutRequest    { string topic = 1; int32 partition = 2; bytes frames = 3; int64 base_offset = 4; }
message ReplicaPutResponse    { int64 high_watermark = 1; }

service ReplicaService {
  rpc ReplicaFetch(ReplicaFetchRequest) returns (ReplicaFetchResponse);
  rpc ReplicaPut(ReplicaPutRequest)     returns (ReplicaPutResponse);
}
```

**ReplicaFetch** returns the raw, concatenated bytes of frames in `[first_offset, last_offset]` (inclusive; clamped to the local high watermark). Frame layout, big-endian, CRC32.IEEE over everything after the CRC field:

```
[len u32][crc u32][offset i64][ts i64][keyLen u32][valueLen u32][key][value]
  len    = bytes after the len field (crc through value)
  offset = absolute log offset of the record
```

**ReplicaPut** applies those frames to this node's log. Out-of-order pushes (a frame whose offset is ahead of the local high watermark) are rejected with `FAILED_PRECONDITION` — never silently skipped. Block identity is content-addressed: `block_id = SHA-256(raw frame bytes)`; descriptor equality across nodes is checkable, and a mismatch is a hard error.

Synthetic example (byte counts abbreviated):

```
ReplicaFetch(topic="orders", partition=0, first_offset=0, last_offset=127)
→ ReplicaFetchResponse{ frames: <3432 bytes: 128 frames, offsets 0..127> }

ReplicaPut(topic="orders", partition=0, base_offset=0, frames=<same bytes>)
→ ReplicaPutResponse{ high_watermark: 128 }
```

### QuorumService

```proto
message QuorumMessageRequest  { bytes payload = 1; }  // JSON quorum.Message
message QuorumMessageResponse {}

service QuorumService { rpc QuorumMessage(QuorumMessageRequest) returns (QuorumMessageResponse); }
```

The payload is a JSON `quorum.Message` — one Raft RPC per message. `type` values: `0` RequestVote, `1` RequestVoteReply, `2` PreVote, `3` PreVoteReply, `4` AppendEntries, `5` AppendEntriesReply.

Example — PreVote from n1 to n2:

```json
{"type":2,"from":"n1","term":7,"candidate_id":"n1","last_log_index":18,"last_log_term":7}
```

Example — AppendEntries carrying a `topic-create` entry:

```json
{
  "type": 4,
  "from": "n2",
  "term": 7,
  "leader_id": "n2",
  "prev_log_index": 18,
  "prev_log_term": 7,
  "entries": [
    {"index": 19, "term": 7, "type": 1, "data": "eyJ0b3BpYyI6Im9yZGVycyIsInBhcnRpdGlvbnMiOjMsIm1vZGUiOiJzdHJlYW0ifQ=="}
  ],
  "leader_commit": 19,
  "success": true
}
```

`entries[].type` values: `0` EntryNoop, `1` EntryTopicCreate, `2` EntryBlockCommit, `3` EntryLeaseGrant; `data` is base64 JSON of the materialized entry (e.g. `{"topic":"orders","partitions":3,"mode":"stream"}` for topic-create).

Failure modes operators actually see: a partition leader that loses quorum steps down; followers reject `AppendEntries` with `success:false`; block commits and lease grants only apply once the current-term entry is committed by a majority.

## `firn-cli` command ↔ API map

| CLI | Path |
|---|---|
| `create-topic` / `create-queue` | `POST /topics` |
| `list` | `GET /topics` |
| `produce` | gRPC `Produce` |
| `fetch` | gRPC `Fetch` |
| `groups` | `GET /groups` |
| `dlq list` | gRPC `Fetch` on `<queue>.dlq` |
| `dlq redrive` | gRPC `Fetch` (dlq) + `Produce` (source queue) |
| `create-apikey` / `revoke-apikey` / `list-apikeys` | `POST /apikeys` / `DELETE /apikeys/{name}` / `GET /apikeys` |
| `federation add-peer` / `list-peers` / `remove-peer` / `keys` / `status` | `POST /federation/peers` / `GET /federation/peers` / `DELETE /federation/peers/{name}` / `POST /federation/keys` / `GET /federation/cursors` |

## Audit trail

`__audit-0` (replicated like any internal topic) holds one record per admin mutation and authz refusal. Frame layout per record (big-endian):

```
u32 bodyLen | u32 crc32.IEEE(body) | u64 offset | u64 ts_ms | u32 keyLen | u32 valueLen | key | value
```

Decode with:

```sh
python3 - <<'EOF'
import json, struct, zlib, glob
for path in glob.glob("data/__audit-0/*.log"):
    with open(path, "rb") as f:
        data = f.read()
    pos = 0
    while pos + 8 <= len(data):
        bodyLen, = struct.unpack_from(">I", data, pos)
        off, ts = 8, pos + 4
        body = data[pos + 4: pos + 4 + bodyLen]
        if len(body) < bodyLen: break  # torn tail
        if zlib.crc32(body[4:]) != struct.unpack_from(">I", body, 0)[0]: break
        off, ts, klen, vlen = struct.unpack_from(">QQII", body, 4)
        key = body[28:28 + klen]; val = body[28 + klen:28 + klen + vlen]
        print(json.dumps(json.loads(val)))
        pos += 4 + bodyLen
EOF
```

Payload shape: `{"ts":…,"who":"<key name|anonymous>","action":"topic-created|apikey-created|…","target":"…","result":"…"}`.

Cluster-mode limitation (honest): `__audit` is a single-writer fenced internal topic like the others, so admin mutations executed *on the lease holder* are audited; mutations executed on non-holder nodes that complete without an internal write — notably topic creates (they go through the quorum proposal on any node) and authz refusals — are **not** audited there; the handler logs the refusal to stderr instead. If you need complete refusal forensics, run admin traffic through the holder or scrape all nodes' logs.

Note: the frame prefix is internal and unversioned; treat any decoder copied from this page as an example, not a stable format.
