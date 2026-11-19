# Firn integrations — ingress, egress, and data pipelines

Firn ships three companion processes that move data in and out of the broker without touching its storage path. They talk to `firnd` through the client gRPC API exactly like any other client, so all broker guarantees (durability, fencing, auth, rate limits) apply unchanged. Design records: ADR-016 (ingress), ADR-017 (egress), ADR-018 (pipelines); delivery plans: `docs/plans/007|008|009-*.md`.

| Binary | Purpose |
|---|---|
| `firn-ingest` | HTTP(S) ingest endpoints, MQTT 3.1.1 publish listener for IoT devices, and a reference file-tail source connector |
| `firn-egress` | Forward selected topics/queues to external systems: JSONL files (stage for data-lake/Databricks auto-loader paths) and HTTP(S)/webhook endpoints (incl. IoT) |
| `firn-pipeline` | Declarative transformation pipelines: filter/project/map/enrich/route, plus opt-in AI-assisted stages |

All three are **at-least-once**. None of them provides exactly-once end-to-end; duplicates on crash/retry are possible and are documented per sink/stage. Offsets are committed (queue acks sent) only **after** the external system durably accepted the data (`after_sink_confirm`).

Non-negotiables (all binaries): no storage-path code, no wire compatibility layer, no implicit broker-side delivery, secrets only via environment variables, fail-closed config validation (unknown fields rejected, auth required unless explicitly disabled).

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
firn-ingest -validate http-ingest.json
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

Minimal MQTT 3.1.1 **publish-only** endpoint (MQTT 5.0 clients connect in 3.1.1 mode). Devices connect, authenticate, and `PUBLISH`; subscriptions are refused (`SUBACK 0x80`) — there is no broker fan-out here.

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

**Not built (dependency-gated, see ADR-016 and plan 007):** Kafka/Redpanda/NATS source connectors and SQL CDC (Postgres logical replication) / NoSQL change-stream connectors. They require third-party client libraries and an explicit dependency decision, which has not been made.

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

### Sinks

**`file` (JSONL staging — flat files for data-lake / Databricks Auto Loader paths)**
Each batch lands as `<dir>/<route>/topic-p<partition>-<first>-<last>.jsonl` written to `*.tmp`, fsynced, then renamed; a `*.manifest.json` sidecar records `{route, partition, first_offset, last_offset, count, crc32c}`. Re-delivery of the same offset range is skipped idempotently. Records are base64-encoded (`value`/`key`, with `*_encoding: "base64"` markers) to guarantee lossless bytes.

**`http` (webhook / IoT endpoint / generic HTTP push)**
`POST` body: `{"route","source","partition","first_offset","last_offset","records":[{key_b64,value_b64,offset,headers}]}`.
Headers: `Idempotency-Key: <route>:<source>:<partition>:<first-last>` (stable across retries), plus auth: `bearer_env` (`Authorization: Bearer`), `hmac_env` (`X-Firn-Signature-256`), or `none`. `429`/`5xx`/network/timeout → retried up to `retry.budget` with exponential backoff; other `4xx` → dead-lettered immediately.

### Semantics and limits

- **Ack policy**: only `after_sink_confirm` exists (offsets committed / queue acks sent after sink success). The sketched `on_write` policy was dropped as redundant.
- **Queue sources** pull through Firn queue semantics (`Pull`/`AckBatch`), so DLQ-after-3-attempts is handled by the broker before data reaches the forwarder.
- **Topic sources** use a dedicated group (`firn-egress-<route>` by default) with generation-0 offset commits (monotonic advance, no generation fencing). **One forwarder instance per route**; scaling out means partitioning routes, not adding replicas to the same group (there is no rebalance yet).
- **v1 limits**: one source per route; no S3/Databricks-direct sinks (files are the staged delivery); no multi-tenant ACLs.
- **Backpressure**: a stalled sink pauses consumption for that route; backlog accumulates in Firn (bounded by retention).
- Observability: structured logs per route (`running`, `deliver error`, DLQ events). Prometheus metrics for routes are not wired yet — alert on log lines + `firn fetch` lag until they are.

**Not built (dependency-gated, see plan 008):** Kafka-compatible messaging sink, S3-compatible object-store sink, direct Databricks Files/UC API client.

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

### Sink types (2026-11-19): `file` | `http` | `databricks` | `s3`

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

Kafka/NATS/Redpanda source and sink connectors and SQL CDC (Postgres logical replication) / NoSQL change-stream connectors remain **dependency-gated** and are not built. Route management mutations in `firn-cli` (there is no control plane to mutate) and per-stage pipeline latency histograms are not built. MQTT 5.0 native and multi-instance checkpoint coordination are not built. The S3 sink is stub-tested, not validated against a real AWS account.
