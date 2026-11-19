# ADR-017: Egress integrations — forwarding topics and queues to external systems

Status: accepted · 2026-11-19 · Implementation proceeds via `docs/plans/008-egress-integrations.md`.

## Context

Firn stores and serves records via streaming topics and queues (ADR-001), but many deployments need data to flow onward: durable archival to files/object storage, analytics sinks (e.g. data lakes/warehouses such as flat-file drops or Databricks ingestion paths), downstream messaging systems, and delivery to external devices or services (including IoT endpoints). Without a defined egress model, users build one-off consumers that duplicate offset management, retry logic, delivery tracking, and failure handling, often with inconsistent correctness guarantees.

ADR-016 defines how external data enters Firn through gateways and source connectors outside the broker core. This ADR defines the symmetric outbound path: controlled, observable forwarding from selected Firn topics and/or queues to external systems.

## Decision

### 1. Boundary: egress runs outside the broker core

Egress integrations (sink connectors and forwarders) run as separate processes or a managed integration runtime that consume from Firn using the official client API/SDK (`sdk/firngo` or generated gRPC clients) and write/deliver to external systems. They:

- **MUST NOT** be implemented inside `firnd`, `internal/log`, `internal/broker`, or storage paths.
- **MUST NOT** read segment files directly; they consume via `Fetch`/group or queue `Pull` APIs so they observe the same HWM, fencing, and queue-ack semantics as any other client (ADR-003, ADR-004).
- **MUST** respect Firn authentication and transport security (ADR-011) using least-privilege credentials.

Forwarding is therefore a client of Firn, not a new storage or replication feature (distinct from intra-cluster replication, ADR-007, and inter-cluster federation, ADR-010).

### 2. Selection and routing model

Operators configure egress **routes** (forwarding rules) that bind:

- **Source selection:** one or more Firn topics (with optional partition filters) and/or queues to consume from.
- **Consumer identity:** consumer group identity for topic streams (offset commits per ADR-001/004) or queue pull/ack behavior for queue sources, with explicit concurrency/parallelism settings.
- **Destination:** a named sink/target system and its connection/configuration.
- **Delivery policy:** batching/windowing, ordering expectations, retry/backoff, timeout, and dead-letter/error handling.
- **Mapping/filtering:** optional record filtering, header/key propagation, and transformation hooks (see ADR-018).

Routes are declarative, versioned configuration objects (format TBD in the implementation plan). A route MUST be enable/disable-able and observable independently. Changes to routing (destination, mapping, selection) MUST be applied in a way that makes the effective configuration and its revision identifiable in logs/metrics.

### 3. Sink categories and destination semantics

The egress model supports pluggable sink connectors. Representative destination categories in scope:

| Category | Examples | Delivery notes |
|---|---|---|
| **File/object storage** | Local/NFS flat files, object storage drops (e.g. S3-compatible), staged files for warehouse ingestion | Records written as batches/objects with deterministic naming/partitioning schemes and manifests/checkpoints so sinks can reason about completeness. |
| **Analytics platforms** | Databricks ingestion paths, lakehouse/warehouse loaders (via files, APIs, or platform connectors) | Typically at-least-once batch delivery; exactly-once into the analytics system depends on that system's ingest/idempotency and is outside Firn's guarantees. |
| **Messaging systems** | External brokers/queues/streams (Kafka, AMQP, cloud messaging, etc.) | Preserve keys/ordering only where both Firn partitioning/group semantics and the destination support it; mappings MUST be documented. |
| **Devices/services/IoT** | HTTP(S) callbacks, device gateways, IoT endpoints | Per-delivery acknowledgments/timeouts and idempotency expectations MUST be explicit. |

A concrete sink implementation MUST document: acknowledgment meaning (when Firn offsets/acks are advanced relative to destination confirmation), ordering guarantees, batching, maximum in-flight records, and behavior on partial batch failure.

### 4. Delivery semantics, offsets, and acknowledgments

- Egress forwarding is **at-least-once** by default: a record may be delivered to a destination more than once across retries, restarts, or rebalances. Exactly-once delivery to an external system is a non-goal of Firn and, where claimed, MUST be qualified as dependent on destination idempotency/transactional capabilities and explicitly tested.
- **Topic sources:** offsets/commits MUST advance only according to the route's configured acknowledgment policy (e.g. after durable sink confirmation or successful batch write). Commit cadence and crash/restart behavior MUST avoid silent data loss; duplicates on recovery are acceptable per at-least-once.
- **Queue sources:** queue `Ack`/completion MUST occur only after the destination has durably accepted (or terminally handled) the message per policy. Visibility timeout/lease interactions MUST be configured so forwarding latency does not cause unintended redelivery storms; redelivery remains governed by Firn queue semantics (ADR-001).
- **Ordering:** per-partition order can be preserved only when a route processes a partition with a single in-flight delivery chain to an order-preserving destination. Parallelism, retries, and multi-partition merges may relax ordering; the effective guarantee MUST be declared per route/sink.
- **Backpressure:** sinks that are slow or unavailable MUST trigger bounded buffering and/or throttled consumption according to policy; unbounded memory growth is forbidden. Persistent backlog remains in Firn (the log/queue), not hidden in the forwarder.

### 5. Error handling and dead-lettering

- Transient destination failures MUST be retried with bounded backoff and jitter.
- Permanent or exhausted-retry failures (schema/validation rejection by sink, authorization failure, non-retriable response, mapping error) MUST be routed to a configured dead-letter destination (Firn topic/queue or external error sink) with error context (route id, source topic/queue/partition/offset or queue delivery id, attempt count, error). Records MUST NOT be silently dropped.
- Poison-record handling MUST be compatible with queue DLQ semantics when the source is a queue (no bypass of Firn's attempt/DLQ accounting except by explicit, documented policy).

### 6. Security and operations

- Destination credentials and Firn credentials are separate; secrets MUST be supplied via secure references and MUST NOT be logged (ADR-011).
- Sink connectors SHOULD use least-privilege Firn read/consume/ack permissions for the selected sources.
- Required observability per route: records consumed, delivered, retried, dead-lettered, in-flight, end-to-end lag (source offset/HWM or queue age vs sink-confirmed position), and destination error rates/latency.
- Administrative actions on routes (create/update/enable/disable/delete) MUST be authenticated, authorized, and auditable if exposed via a managed control plane.

### 7. Extensibility and dependencies

- Sink implementations SHOULD conform to a small, stable interface (validate/open/write-batch-or-send/confirm/close/health) to allow new destinations without broker changes.
- Destination drivers/SDKs (cloud storage, Databricks/warehouse clients, messaging clients, HTTP stacks beyond stdlib where justified) are third-party dependencies subject to repository rules: no dependency added without an explicit recorded decision/ADR context and human approval; prefer stdlib and isolate optional drivers behind separate modules/binaries/build tags.

## Consequences

- Firn gains a first-class, configurable path to archive and forward data to storage, analytics, messaging, and device endpoints while keeping the broker core unchanged.
- Operators take on lifecycle management of forwarder/connector processes and must capacity-plan for sink outages (Firn retains backlog; forwarders add compute/network).
- Correctness hinges on disciplined offset/ack advancement relative to sink confirmation; implementations require conformance tests for crash/retry/duplicate behavior.
- Destination-specific limitations (transactionality, idempotency keys, file commit protocols, rate limits) will constrain achievable guarantees; documentation must be explicit per sink to avoid false exactly-once assumptions.
- Federation (ADR-010) and egress solve different problems (cluster-to-cluster replication vs. delivery to external systems) and may coexist; operators must avoid accidental feedback loops when forwarding to systems that also ingest back into Firn.

## Non-goals

- Implementing sink delivery inside `firnd` or modifying storage/replication internals.
- Exactly-once delivery to arbitrary external systems or distributed transactions across Firn and destinations.
- Replacing federation (ADR-010) with sink connectors, or vice versa.
- A universal exactly-once file-to-warehouse commit protocol in v1 of this feature (sink-specific capabilities apply).
- Unbounded buffering, silent drops, or implicit reordering without declaration.

## Implementation notes (planned)

- Deliver via a plan (slices): (1) route/config schema, offset/ack policy model, and sink contract, (2) reference file/object-storage sink with deterministic batching/manifests, (3) reference messaging/HTTP sink, (4) analytics-oriented sink integration (e.g. Databricks-compatible staged delivery) as a connector with documented guarantees, (5) observability and DLQ conformance suite.
- Each slice that introduces new dependencies or new outbound trust relationships requires explicit review and approval per repository rules.
- This ADR depends conceptually on ADR-016's integration boundary and composes with ADR-018 for transformation; it does not amend storage invariants or ADR-007/010 replication semantics.

## Implementation notes (2026-11-19): reference implementations shipped

What shipped vs the decision record, honestly:

- **`firn-egress` (new binary).** Routes are validated JSON (unknown fields rejected; topic XOR queue per route; one source per route; `ack_policy` must be `after_sink_confirm` — the sketched `on_write` was dropped as redundant).
- **File sink** (`type: file`): JSONL batches `{route}/{src}-p{partition}-{first}-{last}.jsonl` written `*.tmp` → fsync → rename; sidecar `*.manifest.json` (`first_offset/last_offset/count/crc32c`); records base64 with explicit `*_encoding` markers; re-delivery of the same offset range is skipped idempotently. This is the staged path for flat-file and Databricks Auto Loader ingestion.
- **HTTP sink** (`type: http`): `POST {route, source, partition, first_offset, last_offset, records[]}`; lossless base64 payloads; `Idempotency-Key` stable across retries; `bearer_env` / `hmac_env` / `none` auth; `429`/`5xx`/timeout/network retried with backoff (budget 5 default); other `4xx` dead-lettered immediately.
- **Offsets**: topic sources commit group offsets with generation 0 (monotonic advance; fencing intentionally off) under a dedicated group (`firn-egress-<route>`). **One forwarder instance per route.** Queue sources use `Pull`/`AckBatch` (broker DLQ handles poison messages before the forwarder sees them). An unconfirmed batch is re-served until `Confirm` succeeds (this fix landed with the slice: advancing positions at fetch time would have silently skipped failed batches in-instance).
- **Not built (dependency-gated):** S3-compatible object-store sink, direct Databricks Files/UC API client, Kafka messaging sink, mTLS client-cert auth for the HTTP sink, multi-topic fan-in per route, Prometheus metrics for routes, `firn-cli` subcommands. Each is named in plan 008 and tracked in ADR-013.
- At-least-once end-to-end everywhere; sinks dedupe by construction (file) or by idempotency key (HTTP); no storage-path or replication changes.

## Implementation notes (2026-11-19b): S3 sink, Databricks sink, lag, metrics, firn-cli surface

Second batch, still honoring "runtime remains outside firnd":

- **`s3` sink shipped** — minimal hand-rolled AWS Signature V4 (PUT only, path-style and virtual-hosted addressing; optional `endpoint` for S3-compatible/MinIO via `force_path_style`, blocked on http unless `insecure_http`). No AWS SDK dependency. Credentials from env only. Keys `{prefix}/{route}/{file}` are a pure function of the offset range ⇒ retry-safe. **Verified against a conformance stub** (signature structure, determinism, error→terminal/retry mapping); not yet run against a real AWS account — validate before production use.
- **`databricks` sink shipped** — UC Volumes Files API (`PUT .../fs/files/...?overwrite=true`), stdlib only, `.jsonl` then `.manifest.json`, both overwrite for retry idempotency; manifest gating documented (no atomic rename over the API).
- **Lag reporting**: `firn_egress_lag_records{route,topic,partition}` gauge (refresh 15s) plus `firn-egress -lag` and `firn-cli integrations lag|validate`; estimates are `committed → scanned HWM`, capped (`+` marker at 10k). Queue sources report lease-managed position (no scan).
- **Prometheus metrics on all three binaries** (`-metrics-addr`): records-in/produced, DLQ totals, op latency histograms, MQTT connection gauge.
- **Databricks staged path** now has two options: slice-2 file sink (unchanged) and direct UC Volumes PUT.
- **Still not built (dependency-gated):** Kafka messaging sink (driver approval needed).

## Implementation notes (2026-11-19c): Kafka sink delivered

- **`kafka` sink**: franz-go producer with `acks=all`; key/header passthrough; plain/SCRAM-SHA-256/SCRAM-SHA-512 auth; optional TLS CA env (`ca_env` → PEM); all produce errors retryable; live-verified against the in-memory fake cluster (kfake) in-tree. Delivery remains at-least-once per route policy. No exactly-once producer session games (idempotent producer not enabled — the ack policy `after_sink_confirm` is the contract).
