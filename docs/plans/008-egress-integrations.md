# Plan 008: Egress integrations (ADR-017)

Derived from ADR-017 (accepted 2026-11-19). One slice per session, same loop as plans 001–006:

> plan changes land here first → tests first → implement → `go vet ./... && go test ./... -race` green → human review → commit.

Non-negotiables: forwarding runs outside `firnd` as a client of the Firn API (ADR-002); no storage/replication/federation changes; offsets/acks advance only after durable sink confirmation per route policy; at-least-once is the documented default; poison records go to a configured DLQ with lineage metadata; destination credentials and Firn credentials are separate trust domains; no dependency without approval recorded here.

Scope note: ships a reference file/object-storage sink and an HTTP(S)/IoT webhook sink (both stdlib-feasible) first; external messaging and Databricks/analytics sinks follow behind dependency review. Egress is distinct from federation (ADR-010): federation moves Firn↔Firn; egress delivers to non-Firn systems.

## Slices

| Slice | Delivers | Review gate |
|---|---|---|
| 1 (**delivered 2026-11-19**) | Route model + sink contract: versioned route config (**JSON**), `after_sink_confirm` ack policy only (`on_write` dropped as redundant — documented), sink interface (`Write/IsTerminal/Close`), topic sources via generation-0 monotonic group commits, queue sources via `Pull`/`AckBatch`, DLQ contract + idempotent confirm (unconfirmed batch re-served) | API + design review — done (in-session, autonomous) |
| 2 (**delivered 2026-11-19**) | File sink: JSONL batches, per-partition split, `tmp`→fsync→rename, `*.manifest.json` (route/partition/offset range/count/CRC32C), base64 lossless encoding, idempotent re-delivery. **S3 backend delivered 2026-11-19 via stdlib SigV4 PUT** (no AWS SDK; env-only creds; stub-tested) | S3 dependency approval — moot (no dep taken) |
| 2b (**delivered 2026-11-19**) | **Databricks UC Volumes Files API sink** (stdlib `net/http`): PUT `.jsonl` then `.manifest.json` with overwrite=true; manifest gating; no atomic rename (documented) | review — done |
| 3 (**delivered 2026-11-19**) | HTTP(S)/IoT webhook sink: POST batches, `Idempotency-Key` stable across retries, bearer/HMAC auth, bounded timeout, retry+backoff (budget 5), 4xx→DLQ | security review — done (in-session, autonomous) |
| 4 (**delivered 2026-11-19**) | External messaging sink: **Kafka sink shipped (franz-go, ADR-019)** — acks=all, SASL plain/scram, TLS optional; live-verified against kfake | review — done |
| 5 (**delivered 2026-11-19**) | Analytics sink: staged-file path (slice 2) unchanged; **direct Databricks endpoint shipped as `databricks` sink (stdlib Files API, no dependency)** | review — done |
| 6 (**delivered 2026-11-19**) | Route surface: `firn-egress -config … [-route name] [-validate] [-routes] [-lag] [-metrics-addr]` + `-debug` pprof (loopback only); **`firn-cli integrations validate` and `firn-cli integrations lag`** shipped; **`firn_egress_lag_records` gauge** (15s refresh); mutations deliberately absent (no control plane) | API review — done |
| 7 (**delivered 2026-11-19 + 2026-11-19b**) | `docs/integrations.md` (semantics, DLQ redrive, security, capacity, databricks/s3 sinks, lag, metrics), ADR-017 validation addendum + 2026-11-19b addendum | docs review — done (in-session, autonomous) |

## Delivery notes (2026-11-19)

- `internal/egress`: route validation (unknown fields rejected), `TopicSource` (resume from committed group offsets; positions advance only at `Confirm`; unconfirmed batch re-served), `QueueSource` (`Pull`/`AckBatch`), file + HTTP sinks, `Runner` with retry budget + DLQ (`after_sink_confirm`).
- `cmd/firn-egress` binary: `-config`, `-route`, `-validate`, `-routes`, `-debug` (pprof, off by default).
- Offset commits use generation 0 by design (monotonic; fenced groups not used); dedicated group `firn-egress-<route>`. One forwarder instance per route — no rebalance semantics yet.
- File sink is the recommended staged delivery for Databricks Auto Loader; manifest gives downstream batch boundaries.
- Deferred deliberately: S3/Databricks-direct/Kafka sinks, route metrics, `firn-cli` integration, multi-topic fan-in (validator rejects >1 source per route today).

## Slice details

### Slice 1 — route model & sink contract (no sinks yet)

- New module `integrations/` shared with plan 007 (decide mono-module vs separate binary; record here).
- Route config validation: unknown fields rejected; referenced topics/queues checked via `GetMetadata` at enable time (non-fatal warning if unreachable, fatal if resolvable and absent); partition filters validated against actual counts.
- Delivery semantics encoded explicitly:
  - `ack_policy: after_sink_confirm` (default) — group offsets committed / queue acks sent only after sink `Confirm` returns success.
  - `ack_policy: on_write` — after `WriteBatch` returns (weaker, for sinks whose durability is known-good; documented).
  - Batch size + max in-flight + retry budget; exceeding retry budget → DLQ with `{route, source, partition, offset|delivery_id, attempts, error}`.
- Conformance harness replays a fault matrix (crash before confirm, crash after write before commit, sink 5xx/timeout, partial batch failure) against any sink implementation.
- Gate: API + design review.

### Slice 2 — file/object-storage sink

- Output: newline-delimited JSON records (`<record>.jsonl`). Each batch: write `*.tmp` → fsync → rename (or object-store put with consistent-read assumption documented); manifest `*.manifest.json` with `{route, topic, partition, first_offset, last_offset, count, crc32c, records_sha256}`.
- Naming: `s3://bucket/prefix/<topic>/<partition>/<startOffset>-<endOffset>.jsonl` — idempotent re-delivery produces identical bytes for identical offset range (dedupe-friendly downstream).
- S3-compatible client: `aws-sdk-go-v2` candidate — **requires explicit dependency approval**; local filesystem mode works with stdlib only and ships in the same slice.
- Consumer position: group commit after object visible; reopen on failure replays at most one batch (duplicates tolerated downstream, documented).
- Metrics: `firn_egress_bytes_total`, `firn_egress_batches_total{result}`, `firn_egress_lag_records`.
- Gate: dependency approval (S3) + review. Local-fs path reviewable independently.

### Slice 3 — HTTP(S)/IoT webhook sink

- Request: `POST <endpoint>` JSON `{route, batch:[{key,value,ts,headers,offset}]}`; auth via mTLS client cert or `Authorization: Bearer`/HMAC-SHA256 signature header (config-selected); per-destination TLS CA pinning optional.
- Idempotency: `Idempotency-Key: <route>:<topic>:<partition>:<first-offset>:<last-offset>` header on every attempt so receivers can dedupe retries.
- Concurrency: bounded worker pool per destination (default 4, max 32); per-request timeout (default 10s); retry with exponential backoff + jitter; terminal 4xx (except 429) → DLQ immediately; 429/5xx/timeouts → retry up to budget.
- Ordering: per partition, single in-flight chain unless `unordered: true` is set on the route.
- Tests: httptest fault matrix (flaky receiver, 429 storm, slow receiver), idempotency-key stability across retries.
- Gate: security review.

### Slice 4 — external messaging sink (Kafka-compatible)

- Produces to `topic` (config mapping: Firn topic/queue → destination topic), key/value passthrough, optional header passthrough, `acks=all` semantics where destination supports it; ordering preserved per partition when destination partitioning matches.
- Library decision + approval required before coding (`franz-go` / `segmentio/kafka-go` candidates); the sink MUST NOT import broker internals — producer client only.
- At-least-once end-to-end; dedupe via destination-side keys where available.
- Gate: dependency approval + review.

### Slice 5 — analytics sink (Databricks path)

- Primary mechanism: staged files (slice-2 sink) under a prefix watched by Databricks Auto Loader; manifest includes schema hints/headers so the platform can infer evolution without glue code.
- Optional direct path: Databricks Files/SQL Execution API client (approval required) for Volumes landing; credentials via env/file references, never in route config plaintext.
- Guarantees documented honestly: at-least-once into storage; exactly-once **into Databricks tables** only insofar as idempotent Auto Loader file handling is configured by the operator — we do not promise it.
- Gate: dependency approval + review.

### Slice 6 — route management & observability

- `firn-cli integrations route add --file route.yaml|--flags ...` with dry-run `route validate`; `status` shows per-route: state (enabled/draining/faulted), lag (source HWM − sink-confirmed position), last error, DLQ count.
- Lag metric `firn_egress_lag_records{route,topic,partition}` scraped from route status; alert-rule examples in `docs/sre.md`.
- Mutations audit-recorded (`__audit`) via the admin API path when run against a firnd with audit enabled, or via the runtime's own structured log otherwise (documented).
- Gate: API review.

### Slice 7 — docs & ADR-017 validation

- `docs/integrations.md` egress section: route reference, per-sink delivery semantics table, DLQ redrive runbook, capacity planning (backlog lives in Firn; forwarders scale horizontally per route).
- ADR-017 addendum: shipped vs decision record.
- ADR-013 register flip.
- Gate: docs review.

## Open items (tracked, not blocking)

- Push egress from Firn directly (broker-side delivery) — rejected by ADR-017; revisit only via a new ADR with storage review.
- Exactly-once into destinations — non-goal; only destination-side idempotency/transactional support can approach it.
- Backpressure into producers (broker-side quotas) — out of scope; routes should buffer in Firn and slow consumption instead.
- UI page for routes — depends on admin UI scope decision; CLI-first for now.
