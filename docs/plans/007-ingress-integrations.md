# Plan 007: Ingress integrations (ADR-016)

Derived from ADR-016 (accepted 2026-11-19). One slice per session, same loop as plans 001–006:

> plan changes land here first → tests first → implement → `go vet ./... && go test ./... -race` green → human review → commit.

Non-negotiables: integrations run as separate processes/runtimes and talk to Firn only through the client API (ADR-002); no storage-path changes (`internal/log`, `internal/meta`, `internal/broker` internals); no dependency without an approved decision recorded in this plan or a dedicated ADR; at-least-once delivery and durable checkpointing are conformance requirements, not aspirations; secrets never logged.

Scope note: this plan delivers reference implementations for the two most-requested ingress paths (HTTP(S) ingest and MQTT IoT publish) plus a checkpoint framework and reference source connector(s). Database CDC connectors have build-vs-buy pressure and land last, behind explicit dependency and data-correctness review.

## Slices

| Slice | Delivers | Review gate |
|---|---|---|
| 1 (**delivered 2026-11-19**) | Integration contract: connector interface, versioned config schema (**JSON, not YAML** — stdlib constraint, documented), checkpoint abstraction + file-based checkpoint store (atomic rename, `0600`; the `__integration_checkpoints` variant was deferred), DLQ routing contract, conformance tests | API + design review — done (in-session, autonomous) |
| 2 (**delivered 2026-11-19**) | Reference HTTP(S) ingest gateway (`cmd/firn-ingest -http`): authenticated POST/webhook endpoints → route→topic mapping (key derivation, partition selection), produce via Go SDK, per-IP fixed-window rate limit, retry on transient failure, DLQ on permanent failure, byte-lossless base64 DLQ payloads (no metrics yet — tracked) | API + security review — done (in-session, autonomous) |
| 3 (**delivered 2026-11-19**) | MQTT ingress gateway (`firn-ingest -mqtt`): **minimal stdlib MQTT 3.1.1 publish-only** server (no external MQTT library taken; MQTT 5.0 clients connect in 3.1.1 mode), username/password auth via env vars, topic-filter→Firn topic mapping (`+`/trailing `#`), QoS 0/1/2 all at-least-once end-to-end (documented), retained publishes forwarded as regular records, unmapped topic ⇒ connection closed (fail-closed) | security review — done (in-session, autonomous); dependency question moot (no dep taken) |
| 4 (**partial — reference only, 2026-11-19**) | Checkpoint runtime + summary: `integration.Runner` drains any SourceConnector into one topic with checkpoint-after-produce; reference `TailFileConnector` (append-only file, byte offset, truncate-safe re-read) delivered instead of a Kafka connector. **Kafka/NATS/Redpanda sources NOT delivered — dependency-gated** | dependency approval — Kafka still gated |
| 5 (not delivered) | SQL/NoSQL source connectors: CDC-based (Postgres logical replication first) or scheduled polling fallback; NoSQL change-stream connector — each needs its own dependency approval | **dependency approval + data-correctness review** — open |
| 6 (**delivered 2026-11-19**) | Docs: `docs/integrations.md` (all binaries, semantics, security), ADR-016 validation addendum, README/CHANGELOG/AGENTS.md sync | docs review — done (in-session, autonomous) |

## Delivery notes (2026-11-19)

- `internal/integration`: `SourceConnector`/`CheckpointStore` contracts, `FileCheckpointStore` (single-instance, atomic rename), `Runner` (batch produce ≤500, per-record fallback isolating poison records to DLQ), `TailFileConnector` reference. All produce paths go through `sdk/firngo`; storage code untouched.
- HTTP gateway: `internal/ingest/httpgw.go`; config rejects unknown fields and missing auth unless `allow_anonymous` (dev). Responses are `202` only after Firn accepts.
- MQTT gateway: `internal/ingest/mqtt.go`; packet parser/writer is hand-rolled over stdlib `net` with a 1 MiB-ish packet cap, 90 s read deadline, connection cap (default 1000). Reviewed for the publish path only; subscriptions are refused, never silently accepted.
- `firn-ingest` binary with `-http` / `-mqtt` / `-source-file` / `-validate` modes.
- Deferred deliberately: external drivers (all of slice 5, Kafka half of slice 4), per-route Prometheus metrics, `__integration_checkpoints` store, MQTT 5.0 native, multi-instance coordination.

## Slice details

### Slice 1 — contract, config, checkpoints (no network services)

- New module `integrations/` (separate Go module or `cmd/firn-ingest` + `internal/integration`; decision recorded in the slice PR). Broker binary untouched.
- `Connector` interface: `Validate(cfg) error`, `Open(ctx)`, `Poll/Receive(ctx) → batch`, `Checkpoint(ctx, pos)`, `Close()`, `Health()`.
- Config schema v1: `{name, type, source{...}, target{firn address, topic|queue}, mapping{...}, delivery{retries, backoff, dlq}, checkpoint{...}}`. Unknown fields rejected; `validate` subcommand for dry-run.
- Checkpoint store: dedicated compacted topic `__integration_checkpoints` (created on first use, `__`-prefixed, normal produce path) storing `{connector, position}`; conformance tests assert resume-after-restart equivalence and no-loss/no-unbounded-replay under crash injection.
- Connectors MUST NOT touch storage internals; everything goes through `sdk/firngo` or generated stubs.
- Gate: API + design review only.

### Slice 2 — HTTP(S) ingest gateway

- Flags/config: listen addr (default `:9465`, separate from client `:9461`), TLS flags mirroring `firnd`, authN: static bearer token map or HMAC signature header verification (webhook style), optional mTLS for device fleets; per-route rate limit (token bucket, reuse `internal/ratelimit` semantics where importable or reimplement minimal).
- Route rules: `path → topic`, `key_path` (JSON pointer) → record key, constant headers, max body size (default 1 MiB), require-content-type.
- Response contract: `202 Accepted` with `{produce: "committed", partition, offset}` only after Firn produce returns; `5xx`/`429` instruct client retry; body never echoed.
- Metrics: `firn_ingest_requests_total{route,result}`, `firn_ingest_produce_seconds`, `firn_ingest_dlq_total`.
- Tests: conformance harness from slice 1 + authz/rate-limit/crash-safety integration tests (`httptest` + in-process broker fixture).
- Gate: API + security review.

### Slice 3 — MQTT ingress gateway

- MQTT server embedded in the gateway process (library decision required — candidates: `mochi-co/mqtt` or `eclipse/paho`-based stacks; nothing enters the tree before explicit dependency approval recorded here).
- Supported: MQTT 3.1.1 and 5.0; QoS 0 (best-effort produce), QoS 1/2 (ack-after-produce semantics documented as at-least-once end-to-end; no exactly-once claim); retained messages NOT forwarded by default (opt-in flag, last-value-wins produce); clean-start/session semantics documented.
- Topic mapping: MQTT topic filter → Firn topic via config rules (e.g. `devices/+/telemetry → iot-telemetry`), `key` = MQTT client id or payload-extracted key; QoS-granted acks only after Firn `Produce` returns.
- AuthN: username/password from config or file; optional mTLS client certs with fleet CA; anonymous access refused unless `-mqtt-allow-anonymous` (loud, audited) is set.
- Metrics: connections, subscriptions/maps, publishes by QoS/result, produce latency, DLQ.
- Gate: dependency approval first, then security review.

### Slice 4 — checkpoint runtime + reference messaging source connector

- Checkpoint runtime shared by all source connectors: read position at open → produce in batches of ≤500 (reuse `ProduceBatch`, ADR-014/plan 006 slice 2) → commit checkpoint only after produce ack; crash between produce and checkpoint commit ⇒ replay (at-least-once, documented).
- Reference connector: Kafka-compatible source (library decision required — e.g. `segmentio/kafka-go` vs `franz-go`; approval required before coding). Consumer group maps to checkpoint position.
- Conformance tests: kill gateway mid-batch → restart → records present in Firn, no gaps beyond replayed duplicates.
- Gate: dependency approval, then review.

### Slice 5 — CDC / database source connectors

- SQL: Postgres logical replication slot consumer (`jackc/pglogrepl` candidate; approval required) as first-class CDC; fallback scheduled-polling connector (`SELECT ... WHERE updated_at > :wm` with watermark checkpoint) for databases without replication access.
- NoSQL: choose based on demand — Mongo change streams (existing driver candidate) or DynamoDB streams. Do not build both speculatively.
- Payload format: raw change events as JSON bytes; key = source primary key(s) serialized; headers carry source coordinates (table, op, LSN/token).
- Data-correctness review: silent row drops, type coercion, and schema-drift behavior must be documented and tested on a live database fixture (dockertest or equivalent; test-infra dependency decision recorded here).
- Gate: dependency approval + data-correctness review.

### Slice 6 — docs & ADR-016 validation

- `docs/integrations.md` (new): architecture diagram, quickstart per connector, security model (gateway/Firn credential separation), DLQ + redrive runbook, sizing/backpressure notes.
- ADR-016 addendum recording what shipped vs the decision record.
- ADR-013 register flipped accordingly.
- Gate: docs review.

## Open items (tracked, not blocking)

- Whether the integration runtime grows a control plane (central per-route enable/disable, UI) — deferred to plan 008/009 needs or a dedicated ADR.
- Exactly-once source→Firn semantics — remains a non-goal (ADR-016).
- MQTT bridging (Firn→MQTT egress) is plan 008's HTTP/IoT sink, not this plan.
- Multi-tenant gateway authZ (per-route ACLs) — depends on ADR-011 non-goal resolution (per-topic ACLs).
