# Firn integrations — ingress, egress, and data pipelines

Firn ships three companion processes that move data in and out of the broker without touching its storage path. They talk to `firnd` through the client gRPC API exactly like any other client, so all broker guarantees (durability, fencing, auth, rate limits) apply unchanged. Design records: ADR-016 (ingress), ADR-017 (egress), ADR-018 (pipelines); delivery plans: `docs/plans/007|008|009-*.md`.

| Binary | Purpose |
|---|---|
| `firn-ingest` | HTTP(S) ingest endpoints, MQTT 3.1.1/5.0 publish listener; source connectors: file-tail, Kafka, Postgres CDC, MongoDB change streams, DynamoDB Streams, **SQL polling (any RDBMS)**, **RabbitMQ**, **NATS JetStream**, **Redis Streams**, **IBM MQ (REST)** |
| `firn-egress` | Forward topics/queues to external systems: **SQL upsert (any RDBMS)**, JSONL files, HTTP(S)/webhooks, Databricks UC Volumes, S3, Kafka, **RabbitMQ**, **NATS**, **Redis Streams**, **IBM MQ (REST)**, **Elasticsearch**, **ClickHouse** |
| `firn-pipeline` | Declarative transformation pipelines: filter/project/map/enrich/route, opt-in AI stages, multi-input fan-in |

All three are **at-least-once**. None of them provides exactly-once end-to-end; duplicates on crash/retry are possible and are documented per sink/stage. Offsets are committed (queue acks sent) only **after** the external system durably accepted the data (`after_sink_confirm`).

Non-negotiables (all binaries): no storage-path code, no wire compatibility layer, no implicit broker-side delivery, secrets only via environment variables, fail-closed config validation (unknown fields rejected, auth required unless explicitly disabled).

All binaries share these conventions:

- **Config files are JSON.** Unknown fields are rejected at load; missing required fields fail closed. Every secret-bearing value is an environment variable (never inline), so configs are safe to check in.
- **Where the Firn Bearer key comes from**: `firn-ingest` reads `firn.api_key_env` from the JSON config (default env name `FIRN_API_KEY`); `firn-pipeline` takes `-firn-key-env` (default `FIRN_API_KEY`); `firn-egress` is config-only (`firn.api_key_env`). `-firn-insecure` / `firn.insecure: true` dials plaintext — dev only; production dials TLS (`firn.ca_env` for a custom CA) whenever the broker enforces auth.
- **`-metrics-addr 127.0.0.1:PORT`** serves Prometheus metrics on `/metrics` (all binaries).
- **`-validate`** loads and validates configs without producing anything (`-http`, `-mqtt`, `-source-*`, `-config`).
- **`-checkpoint`** (source connectors only): path of the JSON checkpoint file; write-ahead of produces is the at-least-once contract. Back it up with the data.
- **`-dlq`**: dead-letter topic; poison records are diverted there, never silently dropped. Configure it for every production route.
- **Lag**: `firn-egress -lag` or `firn-cli integrations lag` prints committed-offset → scanned-HWM backlog per partition (capped at 10k; `+` suffix when capped).

```sh
firn-ingest -source-kafka kafka.json -topic firn-in -checkpoint /state/cp.json -dlq ingest-dlq \
            -firn localhost:9461 -firn-key-env FIRN_API_KEY
firn-egress -config egress.json -route telemetry-archive -lag
firn-cli integrations lag -config egress.json -route telemetry-archive
firn-cli integrations validate -config egress.json
```

(Note: each `-source-*` method names its JSON config, which holds the connector type's own fields; `-connector-name` exists but only sets checkpoint identity, not the target topic. `-firn-key-env` applies to `firn-ingest`/`firn-pipeline`; `-firn-egress` reads `firn.api_key_env` from its JSON config.)

---

## 1. `firn-ingest` — ingress (ADR-016)

### 1.1 HTTP(S) ingest gateway

Accepts authenticated HTTP(S) `POST`s and produces the raw body to a Firn topic.

```json
{
  "firn": { "addr": "firn.internal:9461", "api_key_env": "FIRN_API_KEY", "ca_env": "FIRN_CA_PATH", "insecure": false },
  "listen": ":9465",
  "tls_cert": "/etc/firn/ingest.crt", "tls_key": "/etc/firn/ingest.key",
  "bearer_env": "INGEST_BEARER_TOKEN",
  "rate_per_minute": 600,
  "max_body_bytes": 1048576,
  "dlq_topic": "ingest-dlq",
  "routes": [
    { "name": "orders", "path": "/ingest/orders", "topic": "orders", "key_json_pointer": "/order_id" },
    { "name": "telemetry", "path": "/hooks/telemetry", "topic": "iot", "key_header": "X-Device-Id" }
  ]
}
```

```sh
firn-ingest -http http-ingest.json
firn-ingest -http http-ingest.json -validate
```

Behavior contract:

- Responses: `202` with `{"produce":"committed","partition","offset"}` only **after** Firn accepts the record; `422` on mapping failures (bad JSON when a key pointer is required); `401`/`429`; `5xx` on Firn failure (retryable); `503` fallback if the DLQ also fails.
- Auth: `bearer_env` (static bearer, constant-time compared) **or** `hmac_env` (`X-Firn-Signature-256: hex(hmac_sha256(secret, body))`), or `allow_anonymous: true` (dev only — refused otherwise).
- Retention of records is the caller's business; the gateway never reads stored data.
- Key: `key_json_pointer` (RFC 6901-ish, `~0`/`~1` decoding supported) or `key_header`. Absent both → round-robin.
- Rate limit: per-IP fixed window (default 120/min, tunable). Body cap 1 MiB default.
- Terminal/validation failures → `dlq_topic` if set; otherwise `422` and the record is acknowledged as lost (it is the caller's to retry) — configure a DLQ.

TLS on the ingest listener is optional; set `tls_cert`/`tls_key`. The Firn dial defaults to TLS (`ca_env` → PEM path, else system roots) unless `firn.insecure: true`.

### 1.2 MQTT ingress gateway (IoT)

Minimal **publish-only** endpoint speaking both MQTT 3.1.1 and the wire-correct MQTT 5.0 surface (CONNECT/CONNACK v5 with properties and reason codes, PUBLISH property sections, QoS 0/1/2 flows, SUBSCRIBE refused with per-filter `0x87`, DISCONNECT reason codes, wildcard/`#` topic mapping). Session state is stateless; subscriptions are refused; extended authentication is refused with `0x86`. There is no broker fan-out.

```json
{
  "firn": { "addr": "firn.internal:9461", "api_key_env": "FIRN_API_KEY", "ca_env": "FIRN_CA_PATH" },
  "listen": ":1883",
  "max_connections": 1000,
  "max_packet_bytes": 262144,
  "users": [ { "username": "fleet-a", "password_env": "MQTT_FLEET_A_PW" } ],
  "dlq_topic": "ingest-dlq",
  "routes": [
    { "name": "telemetry", "pattern": "devices/+/telemetry", "topic": "iot-telemetry", "key_from": "client_id" },
    { "name": "commands",  "pattern": "devices/#",           "topic": "iot-all",       "key_from": "username" }
  ]
}
```

```sh
firn-ingest -mqtt mqtt-ingest.json
```

Behavior contract (all documented, none silent):

- **Wildcards**: `+` single-level, trailing `#` multi-level only.
- **QoS**: 0 → produce as fast as received; 1 → produce then `PUBACK`; 2 → produce on receipt, then `PUBREC`/`PUBREL`/`PUBCOMP`. End-to-end is **at-least-once**; MQTT QoS 2 does not make Firn delivery exactly-once.
- **Retained publishes are accepted and forwarded as regular records** (retain flag ignored). There is no last-value-wins semantics in the broker path.
- **Unmapped topic** → connection closed without `PUBACK` (fail-closed; at-least-once clients retry).
- **Produce failure** → record dead-lettered to `dlq_topic` if set (connection survives); otherwise connection closed so the client retries.
- Auth: per-user `password_env` (constant-time). `allow_anonymous: true` exists for benches and is never the default. Anonymous-only configs are rejected at startup.
- `key_from`: `client_id` (default), `username`, `none` (round-robin).

### 1.3 Tail-file reference connector (source bridge, stdlib)

```sh
firn-ingest -source-file /var/log/app/outbox.ndjson -topic inbox \
            -firn localhost:9461 -firn-key-env FIRN_API_KEY -firn-insecure \
            -checkpoint ./state/outbox.cp -dlq ingest-dlq
```

Emits each complete line of an append-only file as one record; position = byte offset persisted in a JSON checkpoint file (atomic rename, `0600`). Rotation is not detected; truncation resets to 0 (re-reads, at-least-once). Lines without a trailing newline are not emitted until completed. This connector exists to prove and exercise the checkpoint runtime without external dependencies.

### 1.4 Source connectors (delivered 2026-11-19, live-tested)

All source connectors share the checkpoint runtime (`integration.Runner`): positions are persisted **after** Firn accepts the batch; crash ⇒ replay, never silent loss. Every connector speaks Firn through the client API only.

**Kafka** (`firn-ingest -source-kafka kafka.json -topic in -checkpoint cp.json`) — pulls one topic, tracks per-partition offsets in the checkpoint (no Kafka consumer groups). Config: `{"brokers":["b:9092"],"topic":"src","start":"earliest","connector_name":"kafka-in"}`. Live-tested against an in-memory fake cluster (`kfake`) in CI/tests.

**Postgres CDC** (`firn-ingest -source-postgres pg.json -topic in -checkpoint cp.json`) — logical replication slot (`firn_cdc` default) + `firn_cdc_pub` publication (created `FOR ALL TABLES` if the role can); `pgoutput` v1 decoded in-process. Emits `{"schema","table","op":"c|u|d|t","lsn","columns":[{"name","oid","value"}],"key":{...}}` per row change. **Requires `wal_level=logical`** (and a replica the operator manages — slot bloat is on you). Live-tested against `debezium/postgres:16` in this environment.

**MongoDB change streams** (`firn-ingest -source-mongo mongo.json -topic in -checkpoint cp.json`) — `Watch()` on one collection; checkpoint = base64 resume token. Config: `{"uri_env":"MONGO_URI","database":"app","collection":"events","full_document":"updateLookup","connector_name":"mongo-in"}`. **Requires a replica set** (single-node with `replSet` is fine). Live-tested against `mongo:7` here.

**DynamoDB Streams** (`firn-ingest -source-dynamodb ddb.json -topic in -checkpoint cp.json`) — `DescribeStream`/`GetShardIterator`/`GetRecords`; per-shard checkpoints (`TRIM_HORIZON` for new shards, `AFTER_SEQUENCE_NUMBER` for seen shards, parent→child shard continuity). Config: `{"stream_arn":"arn:aws:dynamodb:...","region":"us-east-1","endpoint":"http://127.0.0.1:58000"}` (`endpoint` optional; empty = AWS). Live-tested against `amazon/dynamodb-local` here.

**Any RDBMS (watermark polling)** (`firn-ingest -source-sql sql.json -topic out -checkpoint cp.json`) — one connector, every RDBMS with a `database/sql` driver. Query: `SELECT cols FROM table WHERE watermark_col > ? ORDER BY watermark_col ASC LIMIT batch`. Types: `int64` (sequences, auto-increment pk) or `timestamp` (RFC 3339/ISO 8601 column). Config: `{"driver":"pgx|mysql|sqlserver|oracle|sqlite","dsn_env":"SQL_DSN","table":"schema.table","watermark_column":"id","watermark_type":"int64","key_columns":["id"],"batch":1000}`. **Live-verified here**: SQLite (in-memory, resume semantics), MariaDB 11 (upsert via ON DUPLICATE KEY UPDATE), Oracle Free 23ai (MERGE with FROM dual — Oracle ≤19c compatible). SQL Server locally blocked (arm64 emulation) — same builder, stub-tested.

**RabbitMQ** (`firn-ingest -source-amqp amqp.json ...`) — AMQP 0.9.1; queue (durable) must exist; `auto_ack=false` (ack only after Firn accepts). Config: `{"url_env":"AMQP_URL","amqp_queue":"legacy.orders","prefetch":64,"connector_name":"rmq-in"}`. **Live-verified** here (`rabbitmq:3`).

**NATS JetStream** (`firn-ingest -source-nats nats.json ...`) — durable pull consumer; ack only after Firn accepts. Config: `{"url_env":"NATS_URL","stream":"EVENTS","subject":"evt.>","consumer":"firn-evt","connector_name":"nats-in"}`. **Live-verified** here (`nats:2 -js`).

**Redis Streams** (`firn-ingest -source-redis-streams redis.json ...`) — XREADGROUP consumer group created at entry `0`; XADD MAXLEN ~ 10000. Config: `{"url_env":"REDIS_URL","redis_stream":"events","group":"firn","redis_consumer":"firn-1","connector_name":"redis-in"}`. **Live-verified** here (`redis:7`).

**IBM MQ** (`firn-ingest -source-ibmmq mq.json ...`) — mqweb REST API (stdlib HTTP, no client libraries): peek (GET) → produce → consume (DELETE), one message per call; throughput ceiling is the REST API's. **Stub-tested only** — the IBM MQ image does not run on this arm64 host.

Kafka consumes with `franz-go` (ADR-019); Postgres via `pglogrepl` (+`pgx/v5`); Mongo via the official `mongo-driver/v2`; DynamoDB via `aws-sdk-go-v2`; RDBMS via `database/sql`; RabbitMQ via `rabbitmq/amqp091-go`; NATS via `nats-io/nats.go`; Redis via `redis/go-redis/v9`; IBM MQ via mqweb REST (stdlib). The drivers are pinned in `go.mod`; ADR-019/020 record the decisions. **S3-compatible object storage is covered by the stdlib SigV4 sink (no SDK).**

**Checkpoint format:** every connector emits an opaque position string into the checkpoint file (`0600`, atomic rename); positions only advance **after** Firn accepts the batch. Truncated/rotated files or slot losses fall back to a full re-read (at-least-once).

---

## 2. `firn-egress` — forwarding to external systems (ADR-017)

```json
{
  "firn": { "addr": "firn.internal:9461", "api_key_env": "FIRN_API_KEY" },
  "routes": [
    {
      "name": "telemetry-archive",
      "sources": [ { "topic": "iot-telemetry" } ],
      "ack_policy": "after_sink_confirm",
      "batch": { "max_records": 500, "flush_interval_ms": 1000 },
      "retry": { "budget": 5, "backoff_ms": 100 },
      "dlq": "egress-dlq",
      "sink": { "type": "file", "dir": "/var/lib/firn/egress" }
    },
    {
      "name": "iot-webhook",
      "sources": [ { "topic": "commands" } ],
      "ack_policy": "after_sink_confirm",
      "dlq": "egress-dlq",
      "sink": { "type": "http", "url": "https://hub.example.com/hook", "auth": "hmac_env", "auth_env": "HUB_HMAC", "timeout_ms": 10000 }
    }
  ]
}
```

```sh
firn-egress -config egress.json              # run all routes
firn-egress -config egress.json -route telemetry-archive
firn-egress -config egress.json -validate
```

### Sink types

Every sink below is validated at config load: unknown types/fields rejected, `after_sink_confirm` required, route coherent (one source, existing topics/queues checked via `GetMetadata` where resolvable). Choose by the destination's write model:

| Sink | Use when | Delivery shape | Auth |
|---|---|---|---|
| `file` | data-lake / Databricks Auto Loader / flat-file export | JSONL batches + manifest, idempotent re-delivery | filesystem perms |
| `s3` | AWS/S3-compatible object store | one object per partition-batch; SigV4 (bytes in body, no SDK) | `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`) |
| `databricks` | Unity Catalog Volumes landing zone | PUT .jsonl then PUT .manifest.json (both overwrite=true) | `DATABRICKS_TOKEN` (env) |
| `sql` | any RDBMS you ship row data to | `INSERT ... ON CONFLICT`/`MERGE` upsert (or `append_only: true`), one transaction per batch | DB DSN env (`pgx`/`mysql`/`sqlserver`/`oracle`/`sqlite`) |
| `kafka` | downstream Kafka cluster | `acks=all`; key/header passthrough; `Idempotency` not needed (broker handles) | `sasl` (plain/scram-sha-256/512) + optional `ca_env` |
| `rabbitmq` | AMQP 0.9.1 | persistent messages, broker publish-confirms | `amqp_url_env` |
| `nats` | JetStream (recommended) or fire-and-forget core | JetStream publish w/ broker ack when `nats_stream` set | `nats_url_env` |
| `redis-streams` | Redis Streams consumer group | `XADD MAXLEN ~10000`, one stream per route | `redis_url_env` |
| `elasticsearch` | search index / analytics rollup | `_bulk`, optional `pipeline` and `_id` | `bearer_env` |
| `clickhouse` | columnar analytics | `INSERT ... FORMAT JSONEachRow` | `user_env`/`password_env` (sent as X-ClickHouse-User/Key) |
| `ibmmq` | IBM MQ (REST/no client libs) | POST persistent + `ibm-mq-md-correlationId` | mqweb basic auth |
| `http` | generic webhook/IoT endpoint | POST JSON with `Idempotency-Key`, retries, 4xx→DLQ | `bearer_env` \| `hmac_env` \| `none` |

Complete examples for each:

**`sql`** (dialect-aware upsert; table + key columns required):

```json
"sink": { "type": "sql", "driver": "pgx", "dsn_env": "PG_DSN", "table": "imported.orders",
          "key_columns": ["id"], "columns": ["id", "note", "qty"], "append_only": false }
```

**`rabbitmq`**:

```json
"sink": { "type": "rabbitmq", "amqp_url_env": "AMQP_URL", "amqp_queue": "legacy.orders" }
```

**`nats`** (JetStream recommended for durability; without `nats_stream` it's at-most-once core publish):

```json
"sink": { "type": "nats", "nats_url_env": "NATS_URL", "nats_subject": "events.data", "nats_stream": "EVENTS" }
```

**`redis-streams`**:

```json
"sink": { "type": "redis-streams", "redis_url_env": "REDIS_URL", "redis_stream": "events" }
```

**`elasticsearch`**:

```json
"sink": { "type": "elasticsearch", "url": "http://127.0.0.1:9200", "index": "products",
          "auth": "bearer_env", "auth_env": "ES_TOKEN", "pipeline": "ingest-geo" }
```

**`clickhouse`** (`index` doubles as the table name; `database` optional, default `default`):

```json
"sink": { "type": "clickhouse", "url": "http://127.0.0.1:8123", "index": "fact_sales",
          "database": "warehouse", "user_env": "CH_USER", "password_env": "CH_PASS" }
```

**`ibmmq`** (mqweb REST; no IBM client libraries):

```json
"sink": { "type": "ibmmq", "mqweb_url": "https://mq.internal:9443/ibmmq/rest/v3",
          "mqweb_queue_mgr": "QM1", "mqweb_queue": "LEGACY.QUEUE",
          "mqweb_user": "app", "mqweb_pass_env": "MQ_PASS", "mqweb_ca_env": "MQ_CA_PATH" }
```

**`kafka`** — franz-go producer, `acks=all`:

```json
"sink": { "type": "kafka", "brokers": ["kafka-1:9092"], "topic": "events.ingested",
          "ca_env": "KAFKA_CA_PATH",
          "sasl": { "mechanism": "scram-sha-512", "username": "firn-egress", "password_env": "KAFKA_PASSWORD" } }
```

Kafka semantics: keys pass through unchanged (null-keyed records use Kafka's partitioner). All produce errors are retryable in this implementation; after the route's retry budget the batch dead-letters per policy. `ca_env` is a PEM CA bundle for TLS to the broker (system roots are otherwise used). Live-tested against an in-memory fake cluster in this environment.

SQL semantics (`type: sql`): JSON row records to a table, one explicit transaction per batch. Dialect-aware upsert: pgx/SQLite `ON CONFLICT DO UPDATE`, MySQL `ON DUPLICATE KEY UPDATE`, SQL Server/Oracle `MERGE` with `FROM dual` and parenthesized ON. `append_only: true` for plain INSERT; `key_columns` (unique constraint) required for upsert; `columns` allow-list optional. **Live-verified** here: SQLite, MariaDB 11, Oracle Free 23ai. SQL Server locally blocked (emulation) — same builder, stub-tested.

RabbitMQ (`type: rabbitmq`): durable, persistent, broker publish-confirms. Default exchange routes by queue name. **Live-verified** here.

NATS (`type: nats`): JetStream when `nats_stream` is set — synchronous publish with broker ack (at-least-once); core NATS otherwise — no delivery guarantee (at-most-once). **Live-verified** here.

Redis Streams (`type: redis-streams`): XADD with MAXLEN ~ 10000; one stream per route. **Live-verified** here.

IBM MQ (`type: ibmmq`): publish (POST) with persistent mode + correlationId; single-consumer queues advised. **Stub-tested only** (see ADR-020).

Elasticsearch (`type: elasticsearch`): `_bulk` index; optional `pipeline` and `_id` from record key. Auth: `bearer_env`. **Live-verified** here (`elasticsearch:7.17`).

ClickHouse (`type: clickhouse`): `INSERT ... FORMAT JSONEachRow` via HTTP; optional `user_env`/`password_env` (sends X-ClickHouse-User/Key). **Live-verified** here.

**`file` (JSONL staging — flat files for data-lake / Databricks Auto Loader paths)**
Each batch lands as `<dir>/<route>/topic-p<partition>-<first>-<last>.jsonl` written to `*.tmp`, fsynced, then renamed; a `*.manifest.json` sidecar records `{route, partition, first_offset, last_offset, count, crc32c}`. Re-delivery of the same offset range is skipped idempotently. Records are base64-encoded (`value`/`key`, with `*_encoding: "base64"` markers) to guarantee lossless bytes.

**`http` (webhook / IoT endpoint / generic HTTP push)**
`POST` body: `{"route","source","partition","first_offset","last_offset","records":[{key_b64,value_b64,offset,headers}]}`.
Headers: `Idempotency-Key: <route>:<source>:<partition>:<first-last>` (stable across retries), plus auth: `bearer_env` (`Authorization: Bearer`), `hmac_env` (`X-Firn-Signature-256`), or `none`. `429`/`5xx`/network/timeout → retried up to `retry.budget` with exponential backoff; other `4xx` → dead-lettered immediately.

### Semantics and limits

- **Ack policy**: only `after_sink_confirm` exists (offsets committed / queue acks sent after sink success). The sketched `on_write` policy was dropped as redundant.
- **Queue sources** pull through Firn queue semantics (`Pull`/`AckBatch`), so DLQ-after-3-attempts is handled by the broker before data reaches the forwarder.
- **Topic sources** use a dedicated group (`firn-egress-<route>` by default) with generation-0 offset commits (monotonic advance, no generation fencing). **One forwarder instance per route**; scaling out means partitioning routes, not adding replicas to the same group (there is no rebalance yet).
- **v1 limits**: one source per route; no multi-tenant ACLs; SQL sinks have no bulk-load path (row-by-row, explicit transactions).
- **Backpressure**: a stalled sink pauses consumption for that route; backlog accumulates in Firn (bounded by retention).
- Observability: `firn-egress -metrics-addr 127.0.0.1:PORT` serves Prometheus metrics (`firn_integration_records_in_total`, `records_produced_total`, `dlq_total`, `op_seconds`, `firn_egress_lag_records{route,topic,partition}`), refreshed every 15s; plus `firn-egress -lag` / `firn-cli integrations lag` for one-shot checks. Alert on consumer lag + DLQ growth.

**Not built:** native per-dialect CDC (MySQL binlog etc.); IBM MQ native client (needs the MQ C client); Snowflake/BigQuery/Redshift; route mutation CLI in `firn-cli`. See `docs/migration-playbooks.md` for the migration paths that ARE supported.

---

## 3. `firn-pipeline` — consume, transform, route (ADR-018)

Declarative stages over JSON records. Deterministic stages run first; `route` (if present) must be the last stage and is the only stage that fans out.

```json
{
  "name": "orders-clean",
  "input": { "topic": "raw-orders", "group": "pipeline-orders-clean" },
  "dlq": "pipeline-dlq",
  "stages": [
    { "type": "filter", "field": "event", "op": "eq", "value": "purchase" },
    { "type": "project", "keep": ["user_id", "email", "amount"], "rename": { "user_id": "customer_id" } },
    { "type": "map", "set": { "source": "web" }, "default": { "currency": "EUR" }, "cast": { "amount": "number" } },
    { "type": "enrich", "enrich": { "key_path": "customer_id", "source_file": "customers.jsonl", "match_field": "id", "on_miss": "passthrough", "fields": { "plan": "plan", "tier": "tier" } } },
    { "type": "route", "rules": [
        { "when": { "field": "amount", "op": "ge", "value": 1000 }, "output": "review" }
      ], "default_output": "clean" }
  ],
  "outputs": {
    "clean":  { "type": "firn_topic", "topic": "orders-clean" },
    "review": { "type": "firn_topic", "topic": "orders-review" }
  }
}
```

```sh
firn-pipeline -config pipeline.json                    # run
firn-pipeline -config pipeline.json -validate          # config check
firn-pipeline -config pipeline.json -dry-run -sample in.jsonl  # no writes
```

### Stage reference

| Stage | Fields | Semantics |
|---|---|---|
| `filter` | `field`, `op`, `value` | ops: `eq ne in exists contains gt ge lt le`; non-match → record dropped (counted, not DLQ) |
| `project` | `keep[]`, `drop[]`, `rename{}` | dot-separated paths; `keep` whitelist wins over `drop`; rename after keep/drop |
| `map` | `set{}`, `default{}`, `cast{}` | `set` overwrites; `default` fills only missing; `cast` targets `string\|number\|bool` |
| `enrich` | `enrich{ key_path, source_file, match_field, fields, on_miss, cache_records }` | read-only JSONL reference join; loaded once, hot-reloaded on mtime change; `on_miss`: `passthrough` (default) or `dlq` |
| `route` | `rules[{when:{field,op,value}, output}]`, `default_output` | first match wins; terminal (must be last); multiple named outputs require it |
| `ai` | `ai{ provider, ... }` | opt-in; see below |

### Delivery semantics

- Records failing a stage go to `dlq` (default) as `{"connector":"pipeline:<name>","stage":...,"error":...,"value_base64":...}` with input coordinates — never silently dropped. `on_error: fail` aborts the pipeline instead; per-stage `on_error` overrides the pipeline default.
- Input offsets/queue acks advance **only after** all outputs for the record are durably produced. Crash ⇒ unconfirmed records re-deliver and may be duplicated in outputs (at-least-once).
- Exactly one input per pipeline. Multi-source joins/correlation are a documented follow-up, not in this build.

### AI-assisted stages (opt-in, ADR-018 §3)

```json
{ "type": "ai",
  "ai": {
    "provider": "openai_compatible",          // or "mock" (tests/dev)
    "endpoint": "https://api.example.com/v1", // https required unless insecure_http
    "model": "gpt-4o-mini",
    "api_key_env": "LLM_API_KEY",
    "prompt": "Classify this ticket, reply with JSON: {{payload}}",
    "output_contract": { "category": "string", "priority": "string", "confidence": "number" },
    "on_invalid": "retry",                    // retry (max 3 attempts) | dlq | fail
    "max_calls_per_minute": 60,
    "daily_call_budget": 10000,               // circuit breaker: records DLQ once exhausted
    "timeout_ms": 15000
  }
}
```

- Output is validated against `output_contract` before it touches the record; invalid output → `on_invalid` policy (retries bounded, never unbounded).
- **Mandatory guardrails**: per-minute rate cap, daily call budget with circuit breaker, hard timeout, endpoint must be HTTPS, key via env only, contract validation. Removing any of these is a release blocker.
- Response fields are **merged** into the record under their contract names.
- `provider: "mock"` reads no network: it returns whatever the test/dev handler is configured to (registered in-process); useful for CI and local development.
- No payload or prompt is logged by the runtime. Provider costs accrue on **your** account; set conservative budgets.

---

## Security notes (all three binaries)

- Firn credentials come from env (`FIRN_API_KEY` by default) and grant the binary the same rights as any client. Give integrations **least-privilege keys** (`viewer` keys only work read-only; integrations producing to Firn need `operator`).
- Configs contain **no secrets**: tokens, passwords, HMAC secrets, and CA paths are env vars everywhere in the above schemas.
- Integrations bind their own listeners (`:9465` ingest HTTP, `:1883` MQTT by convention) — keep them on private networks; publishers/sinks authenticate per the gateway config.
- `firn-egress -debug` exposes pprof and is off by default; bind it to loopback only.
- Audit: integration actions show up as ordinary client operations in Firn's logs/audit; add your own structured-log shipping for the integration processes.

### `s3` and `databricks` (details)

**`databricks`** — PUT each batch to a Unity Catalog Volume via the [Files API](https://docs.databricks.com/api/workspace/files) (`{workspace_url}/api/2.0/fs/files{volume_path}/{route}/{file}?overwrite=true`):

```json
"sink": { "type": "databricks", "workspace_url": "https://dbc-123.cloud.databricks.com", "volume_path": "/Volumes/cat/schema/vol/landing", "token_env": "DATABRICKS_TOKEN" }
```

`.jsonl` is PUT first, then `.manifest.json` (both `overwrite=true` — retry-safe). There is no atomic rename over the Files API; consumers should gate on the manifest. Auth is a workspace bearer token from env only.

**`s3`** — minimal AWS Signature V4 signer (no AWS SDK; PUT only), compatible with AWS S3 and S3-compatible endpoints (MinIO via `endpoint` + `force_path_style`):

```json
"sink": { "type": "s3", "bucket": "data-lake", "prefix": "raw/telemetry", "region": "us-east-1" }
"sink": { "type": "s3", "bucket": "lake", "region": "us-east-1", "endpoint": "https://minio.internal:9000", "force_path_style": true }
```

Credentials: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`) from env. Keys are `{prefix}/{route}/{file}` — a pure function of the offset range, so retries are idempotent. Validated against a conformance stub (signature shape, deterministic signing, error mapping); **validate against your endpoint before production use.**

### Lag and metrics (all binaries)

All three binaries accept `-metrics-addr 127.0.0.1:PORT` serving `/metrics`:

- `firn_integration_records_in_total{component,route,source,result}`
- `firn_integration_records_produced_total{component,route,output,result}`
- `firn_integration_dlq_total{component,route,stage}`
- `firn_integration_op_seconds{component,op}`
- `firn_egress_lag_records{route,topic,partition}` (egress only; committed → scanned HWM, capped at 10k per partition, refreshed every 15s)
- `firn_integration_connections_active{component,listener}` (ingest only)

`firn-egress -lag -config egress.json` prints the same lag as a one-shot table (`+` suffix when capped). `firn-cli integrations lag -config egress.json [-route r] [-max-scan N]` does the same from the CLI; `firn-cli integrations validate -config egress.json` validates without running.

### Multi-input pipelines

`"inputs"` (list) replaces `"input"` for fan-in; every input runs under its own consumer group, and each record's doc gains a `_source` field (`topic:name` / `queue:name`) before stages run — correlate sources via `enrich`, never via ordering:

```json
"inputs": [
  { "topic": "raw-orders" },
  { "topic": "fraud-scores" },
  { "queue": "review-jobs" }
]
```

Exactly one of `input`/`inputs` is required (validated). `_source` is also injected in single-input mode.

### Design-time AI mapping assistant (`firn-pipeline -suggest-map`)

```sh
firn-pipeline -suggest-map -sample events.jsonl   -goal "keep user_id, drop email"   -provider openai_compatible -endpoint https://api.example.com/v1 -model gpt-4o-mini -ai-key-env LLM_API_KEY
```

Prints a proposed `project`/`map` fragment for human review — never mutates deployed configs. `-provider mock` runs fully offline for tests/dev.

## Known gaps (tracked in ADR-013)

- SQL Server local verification blocked (arm64 emulation); same builder as Oracle's MERGE.
- IBM MQ: REST-level only; no native client; stub-tested. Live verification needs x86 hardware.
- Deep per-dialect CDC (binlog/LogMiner/SQL Server CDC) is deferred to demand.
- Route mutation CLI in `firn-cli` is not built (no control plane).
- SQL sinks have no bulk-load path (row-by-row, explicit transactions; fine for migration at moderate rates, not >50k rows/s bulk loads).
- Elasticsearch sink is bearer-only; no signing/mTLS to ES yet.
- S3 sink remains stub-tested (no AWS account here).
- MQTT 5.0: publish-only, stateless, no shared subscriptions/extended auth.
