# Plan 009: Data pipelines & transformations (ADR-018)

Derived from ADR-018 (accepted 2026-11-19). One slice per session, same loop as plans 001–006.

> plan changes land here first → tests first → implement → `go vet ./... && go test ./... -race` green → human review → commit.

Non-negotiables: pipelines run outside `firnd` as clients of the Firn API; no storage/replication/federation changes; deterministic stages only in scope until AI stages are explicitly approved; input offsets/acks advance only after outputs are durably produced/forwarded (at-least-once end-to-end); AI stages are opt-in, schema-validated, and never on the critical path of basic pipeline operation; no executable user code/plugins without a dedicated sandboxing ADR.

Scope note: delivers the declarative projection/mapping engine first (the "pick the fields you need" case) with multi-output routing; enrichment and AI-assisted stages land after, each behind its own gate. Plan 007's HTTP/MQTT ingress and plan 008's sinks provide the input/output edges; a pipeline can also run source→Firn→Firn for in-broker-adjacent reshaping.

## Slices

| Slice | Delivers | Review gate |
|---|---|---|
| 1 (**delivered 2026-11-19**) | Envelope model + deterministic stage engine: `filter`, `project`, `map`, config schema + validation (**JSON** — stdlib constraint), conformance-style unit tests. `pipeline.yaml` sketch became `pipeline.json` | API + design review — done (in-session, autonomous) |
| 2 (**delivered 2026-11-19**) | Routing: `route` stage (**terminal**, first-match, required for multiple outputs) → named outputs (`firn_topic`/`firn_queue`); input at-least-once, offsets commit only after outputs durable. `enrich` stage (read-only JSONL reference, `match_field` join, mtime-invalidated cache, `on_miss: passthrough|dlq`) | API + design review — done |
| 3 (**partial — 2026-11-19**) | Pipeline runtime: `firn-pipeline -config … [-validate|-dry-run -sample f.jsonl|-stats-interval 10s]`. **Per-stage Prometheus metrics, revision header injection, and `firn-cli` integration NOT delivered** (firn has no record headers — revision travels in logs/`-revision`; outputs are not mutated) | API + security review — done (in-session, autonomous) |
| 4 (**delivered 2026-11-19**) | AI-assisted stage (opt-in): providers `mock` (tests) and `openai_compatible` via **stdlib HTTP (no new dependency)**; contract-validated JSON output (`output_contract`), bounded retries (`on_invalid: retry` ⇒ max 3), per-minute rate cap (default 60), daily-call circuit breaker (default 10000), HTTPS-only endpoints, env-only keys, `on_invalid: retry|dlq|fail`, fields merged into record; payloads never logged | security review — done (in-session, autonomous); dependency question moot (no dep taken) |
| 5 (not delivered — stretch) | Design-time mapping assistant | review — open |
| 6 (**delivered 2026-11-19**) | `docs/integrations.md` worked examples (PII drop, multi-stage clean, AI extract), ADR-018 validation addendum | docs review — done (in-session, autonomous) |

## Delivery notes (2026-11-19)

- `internal/pipeline`: stage engine (`filter/project/map/enrich/route/ai`), dot-path get/set/delete, ISO-independent JSON semantics (numbers as float64), dry-run mode, revision = config SHA-256 prefix.
- Outputs go through `sdk/firngo` `Produce`/`ProduceBatch`; inputs reuse `internal/egress.TopicSource/QueueSource` (generation-0 commits for topics, `AckBatch` for queues) — same at-least-once discipline.
- Records failing a stage → pipeline DLQ (`{connector, stage, error, value_base64}`); `on_error: fail` aborts the batch instead. Payload logging is off by default (AI stage logs nothing of content).
- Deferred deliberately: multi-source inputs, slice 5 assistant, stage metrics, header-based revision propagation (no headers in firn records).

## Slice details

### Slice 1 — envelope & deterministic stage engine

- Envelope (internal + config-facing): `{key, value, headers, meta{topic, queue, partition, offset | delivery_id, ts, source_routes[]}}`. Pass-through of raw bytes when no stage decodes.
- Stages (YAML):
  - `filter`: `{field, op: eq|ne|in|exists|contains|regex, value}` — drops non-matching records (counted as `filtered`, not errors).
  - `project`: `{keep: [a, b.c], drop: [...], rename: {old.path: new.path}}` — JSON payloads only; non-JSON records rejected to DLQ unless `on_parse_error: passthrough`.
  - `map`: `{set: {field: literal}, default: {field: value}, cast: {field: string|number|bool}}`.
  - Missing field behavior per stage: `error` (DLQ) | `skip_field` | `default` (uses `map.default`).
- Validation at config load: unknown fields/stages rejected; output routing referential integrity checked; regexes compiled; casts checked against samples when `--sample` provided.
- Conformance harness: replay committed input windows → kill runtime → re-run → outputs identical or duplicated at delivery boundary (never lost, never reordered within partition).
- Gate: API + design review. No AI anywhere in this slice.

### Slice 2 — routing & fan-out

- `route` stage: ordered rules `{when: <filter-expr>, output: <name>}`; unmatched records → default output or DLQ per config. Outputs: `firn://topic/partition-by-key|rr`, `queue://name`, `egress://route-name` (plan 008 reference by route id — integration contract finalized with plan 008 slice 1).
- Fan-out semantics: each output independently retried; input offsets/acks commit only after **all** outputs for a record confirm (`after_all_confirm`) or, if configured, `after_any_confirm` with explicit per-output DLQ fallback (documented trade-off: weaker delivery for straggler outputs).
- `enrich` v1: `{lookup: {key_path, source: firn-topic|file, cache_records, ttl}, fields: {...}}` — snapshot copied at startup or on explicit reload; miss → `on_miss: passthrough|dlq|default`. No remote lookups in v1 (latency/failure isolation).
- Gate: API + design review.

### Slice 3 — runtime & operations

- `firn-pipeline` binary (new `cmd/firn-pipeline`): consumes configured inputs (groups per input), executes stage DAG (linear in v1), produces outputs. Graceful shutdown drains in-flight and commits before exit.
- Revision: config content hash + semver string; propagated as `firn-pipeline-revision: <rev>` header; logged per batch; visible in metrics as label (bounded cardinality: revision changes are rare and operator-initiated).
- Metrics: `firn_pipeline_records_total{stage,result}`, `firn_pipeline_stage_seconds{stage}`, `firn_pipeline_dlq_total{stage}`, `firn_pipeline_lag_records{input}`, AI metrics reserved for slice 4.
- Dry-run: `--dry-run --sample-file in.jsonl` prints per-stage output + counts without producing.
- Access log: `{pipeline, revision, input coords, stages applied, result}` — **payload excluded by default**; `-log-payloads` is a loud, audited opt-in.
- Gate: API + security review.

### Slice 4 — AI-assisted stage (opt-in)

- Stage type `ai_extract|ai_classify|ai_map` with `{provider, endpoint?, model, prompt_template, output_schema, timeout_ms, max_calls_per_minute, daily_call_budget, on_invalid: retry|dlq|fail, fallback: <stage-ref>?}`.
- Provider abstraction: `AISProvider { Complete(ctx, Request) (Response, error) }`; ships with one reference adapter (candidate: generic OpenAI-compatible HTTP API — **dependency decision: stdlib HTTP vs official SDK; approval required**); `mock` provider always available for tests/dev.
- Output validation: JSON-schema-check `output_schema` before downstream use; invalid → `on_invalid` policy; all validation failures counted in metrics and DLQ'd with raw request/response metadata (not secrets).
- Guardrails mandatory: hard timeout, token/size caps, per-pipeline rate limiter, daily budget circuit breaker (auto-disables AI stage until operator reset; emits metric + audit event).
- Observability: `firn_pipeline_ai_calls_total{stage,result}`, `firn_pipeline_ai_seconds`, `firn_pipeline_ai_budget_remaining`; no payloads or prompts logged by default.
- Security: provider credentials via env/file only; explicit allowlist of endpoints (no arbitrary URL per pipeline unless `-ai-allow-any-endpoint`, loud opt-in); data-egress disclosure documented in `docs/integrations.md`.
- Gate: dependency approval + security review; shipping without any one of rate limit / schema validation / budget circuit breaker is a release blocker.

### Slice 5 — design-time mapping assistant (stretch)

- CLI: `firn-pipeline suggest-map --sample events.jsonl --goal "keep user_id, email domains, event type"` → emits candidate `project`/`map` YAML. Uses the slice-4 provider with a fixed meta-prompt; output is plain config, validated through the normal loader, always human-approved.
- Never mutates deployed configs; never runs at pipeline runtime.
- Gate: review.

### Slice 6 — docs & ADR-018 validation

- Worked examples: (a) two upstream topics (Kafka CDC + IoT MQTT ingest) → select/rename/join-by-enrich → route telemetry subset to Databricks egress, compliance-relevant subset to queue for alerting; (b) PII field drop at egress boundary; (c) `ai_extract` pulling structured fields from free-text support notes with DLQ-on-invalid.
- Performance guidance: deterministic stages are byte-shuffling (fast path); AI stages dominate latency — budget accordingly.
- ADR-018 addendum: shipped vs decision record.
- ADR-013 register flip.
- Gate: docs review.

## Open items (tracked, not blocking)

- Stateful joins / windowed aggregation across sources — explicitly out of initial scope (ADR-018 non-goal); would be its own ADR + likely a Flink/ksqlDB-class integration rather than in-process engine.
- Exactly-once pipelines — non-goal.
- Arbitrary user-supplied code (starlark/Lua/wasm plugins) — requires separate sandboxing ADR and review before any design.
- Control plane (central pipeline registry across replicas) — CLI-first; service mode deferred until demand exists.
