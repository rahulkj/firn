# ADR-016: Ingress integrations — protocol gateways and source connectors

Status: accepted · 2026-11-19 · Implementation proceeds via `docs/plans/007-ingress-integrations.md`.

## Context

Firn's public data-plane surface today is `FirnService` gRPC plus the admin HTTP API (ADR-002). That is a clean, broker-native interface, but operators and application teams routinely need to bring data into Firn from systems that do not speak Firn's API: IoT devices publishing over MQTT, HTTP producers and webhooks, existing messaging platforms, and operational databases (SQL and NoSQL).

Today this requires bespoke glue outside the project. Without a defined integration model, each ad hoc bridge makes its own choices about authentication, offset/checkpoint handling, error reporting, retries, and how external delivery semantics map onto Firn's at-least-once log and queue semantics (ADR-001, ADR-003, ADR-004). That fragmentation is an operational and correctness risk.

ADR-002 establishes that wire compatibility with foreign protocols is a non-goal for the broker core and that, if needed, compatibility ships as a **separate protocol gateway process** translating foreign requests to the internal/client API — never inside the broker core. This ADR extends that principle to a general ingress integration model.

## Decision

### 1. Boundary: integrations are outside the broker core

Ingress integrations run as separate processes (gateways, connector workers, or a managed integration runtime) that produce into Firn using the official client API/SDK (`sdk/firngo` or generated gRPC clients). They:

- **MUST NOT** be implemented inside `firnd`, `internal/log`, `internal/broker`, or any storage path.
- **MUST NOT** write to topic segment files or internal topics directly.
- **MUST** use the normal produce path so all existing invariants apply unchanged: per-partition monotonic offsets, HWM visibility, lease/federation fences, authn/authz, audit, and rate limiting (ADR-003, ADR-007, ADR-010, ADR-011).

This preserves the "one storage engine, two consumption semantics" design (ADR-001) and keeps protocol and connector complexity out of the correctness-critical core.

### 2. Two ingress classes

| Class | Role | Examples |
|---|---|---|
| **Protocol gateway** | Accepts inbound connections/requests in a foreign protocol and translates them into Firn `Produce` (and, where applicable, queue/ack interactions exposed via the client API) calls. | MQTT publish ingress for IoT devices; HTTPS ingest/webhook endpoints; future protocol frontends. |
| **Source connector** | Actively reads/polls/subscribes from an external system and forwards records into Firn. | Other messaging platforms (e.g. Kafka, AMQP/RabbitMQ, NATS, cloud pub/sub); SQL databases (CDC or polling); NoSQL change streams; files/object-store sources. |

A single integration runtime MAY host multiple connector instances, but each connector instance has an explicit configuration, identity, and lifecycle.

### 3. Protocol gateways

Gateways are standalone processes per ADR-002. Initial target protocols called out by this ADR:

- **MQTT (IoT ingress):** a gateway that acts as an MQTT-facing endpoint for devices and publishes received messages into configured Firn topics/queues. The gateway MUST define and document: MQTT version support (3.1.1 and/or 5.0, declared explicitly per implementation), topic/filter-to-Firn-topic mapping, key/partition derivation, QoS mapping, retained-message handling, session/clean-start behavior, and client authentication/authorization at the MQTT edge. Because MQTT QoS and Firn semantics are not identical, the mapping MUST be explicit: at-most-once MQTT publishes may be forwarded best-effort; QoS 1/2 flows MUST be translated in terms of Firn's at-least-once produce/ack behavior and documented as providing no stronger than at-least-once end-to-end delivery into Firn.
- **HTTP(S) ingest:** a gateway exposing authenticated HTTP(S) endpoints (including webhook receivers) that validate requests, apply routing/mapping rules, and produce to Firn topics/queues. Request acceptance MUST be distinguishable from durable produce acknowledgment so callers can reason about retry behavior.

Gateways MUST authenticate inbound producers using mechanisms appropriate to the protocol (mTLS, tokens, credentials, device identity) and MUST authenticate to Firn using Firn API keys/transport security (ADR-011). Gateway credentials and Firn credentials are separate trust domains.

### 4. Source connectors

Source connectors are configured, long-running workers with the following required properties:

- **Declarative configuration:** each connector instance is defined by a versioned configuration document (format TBD in the implementation plan; YAML or JSON) specifying: connector type, source endpoint/DSN, source credentials reference, target Firn cluster/address, target topic or queue, routing/mapping rules (subject to ADR-018 for transformation), concurrency/polling cadence as applicable, and error-handling policy.
- **Checkpointing:** connectors that read from position-tracked sources (broker offsets, CDC LSN/binlog position, change-stream resume tokens, file offsets) MUST persist checkpoints durably so that restart resumes without unbounded replay or silent gaps. The checkpoint store MUST be explicit in configuration (e.g. a dedicated Firn internal-style topic/stream consumed by the runtime, or an external store) and MUST be documented for failure/recovery behavior. Checkpoint commits MUST be ordered relative to successful Firn produces in a way that preserves at-least-once semantics.
- **Idempotency awareness:** because Firn provides at-least-once delivery/ingest, connectors and downstream consumers MUST tolerate duplicates. Where a source provides stable ids/keys, connectors SHOULD propagate them as record keys or headers to aid downstream deduplication.
- **Schema/data handling:** connectors forward record payloads as opaque bytes unless an explicit decode/mapping step is configured (see ADR-018). Connectors MUST NOT silently drop fields or coerce types without configuration.

Representative source categories in scope for this model (not all required for an initial milestone): messaging platforms, SQL databases via CDC or scheduled query/polling, NoSQL databases via change streams or queries, and file/object-store ingestion.

### 5. Delivery semantics and error handling

- Ingress integrations provide **at-least-once** delivery into Firn unless a specific connector explicitly documents a weaker mode (e.g. best-effort telemetry). Exactly-once end-to-end from an external source into Firn is a non-goal.
- Transient produce failures (network errors, `NOT_CURRENT_LEASE`, backpressure) MUST be retried with bounded backoff according to policy.
- Records that cannot be mapped or produced after policy-defined attempts (poison records, validation failures, oversized payloads) MUST be routed to a configured dead-letter destination (a Firn queue/topic or an external sink) with error metadata; they MUST NOT be silently dropped.
- Per-connector metrics (records read, produced, failed, retried, checkpoint lag, source lag where available) and structured logs are required for operability.

### 6. Security and operations

- Secrets (source credentials, Firn keys, TLS materials) MUST be supplied via references to secure inputs (environment, files with restricted permissions, or secret stores) and MUST NOT be logged. This aligns with ADR-011's fail-closed and no-secret-in-plaintext posture.
- Connectors SHOULD run with least-privilege Firn keys scoped to the target topics/queues they produce to (coarse roles today per ADR-011; finer-grained ACLs remain a non-goal here).
- The integration runtime/control plane, if introduced, is an administrative surface and MUST follow the same authentication, authorization, and audit expectations as other admin surfaces (ADR-011). Its design is an implementation concern to be detailed in a follow-on plan/ADR if it becomes a shipped service.

### 7. Extensibility and dependencies

- Connector implementations SHOULD conform to a small, stable interface (provision/validate/open/poll-or-receive/produce/checkpoint/close/health) so new sources can be added without changing the broker.
- External client libraries/drivers (MQTT brokers/servers, database drivers, messaging SDKs) are third-party dependencies. Per AGENTS.md and ADR-001 conventions, adding any dependency requires an ADR (or an explicit recorded decision within the implementing ADR/plan) and human approval; the reference/initial implementation SHOULD prefer stdlib and already-approved dependencies where feasible, and isolate optional drivers behind build tags or separate modules/binaries.

## Consequences

- Firn gains a clear, supported path for MQTT, HTTP(S), and heterogeneous source ingestion without contaminating the broker core or changing storage semantics.
- A new class of deployable components (gateways/runtimes/connectors) is introduced with its own lifecycle, upgrade, and observability concerns; operators must deploy and monitor them alongside `firnd`.
- The security surface expands (inbound protocol endpoints, outbound connections to sources, credential handling). Each gateway/connector must be reviewed as an exposed service.
- Checkpoint correctness becomes critical: bugs can cause duplicate floods or data loss relative to the source. Checkpoint storage and recovery procedures must be tested and documented per connector.
- Connector driver dependencies and version skew (especially database CDC and proprietary platform clients) will be an ongoing maintenance cost; isolating them from core keeps the broker stable but shifts burden to the integration layer.

## Non-goals

- Implementing MQTT, HTTP ingest, or any foreign protocol inside `firnd` or the storage engine.
- Wire compatibility with any existing broker as a drop-in replacement (ADR-001, ADR-002).
- Exactly-once end-to-end ingestion from external systems, distributed transactions, or cross-system atomicity.
- Automatic schema registry/inference as a requirement (may be layered later; see ADR-018 for transformation/mapping).
- Embedding unvetted third-party connector runtimes into the broker binary.

## Implementation notes (planned)

- Deliver via a plan (slices): (1) integration contract/config schema and checkpoint model, (2) reference HTTP(S) ingest gateway, (3) reference MQTT ingress gateway with documented QoS/topic mapping, (4) initial source connector(s) for a messaging platform and/or database CDC/polling, each with conformance tests for retry/checkpoint/DLQ behavior.
- Each slice that introduces new dependencies or exposed network services requires explicit review and approval per repository rules.
- This ADR does not amend ADR-002 or ADR-003; gateways/connectors are clients of the existing API and storage guarantees.

## Implementation notes (2026-11-19): reference implementations shipped

What shipped vs the decision record, honestly:

- **`firn-ingest` (new binary).** Everything runs outside firnd as a client.
  - HTTP(S) gateway: validated JSON config (unknown fields rejected), bearer or HMAC auth (constant-time; `allow_anonymous` exists for dev and is refused otherwise), per-IP fixed-window rate limit (default 120/min), 1 MiB body cap, `202` only after Firn accepts the record, `422`+DLQ for mapping failures, `Idempotency` n/a (caller-driven). Key via `key_json_pointer` (`~0`/`~1`) or `key_header`; otherwise round-robin.
  - MQTT gateway: a **minimal stdlib MQTT 3.1.1 publish-only** server (no third-party MQTT dependency was taken). Subscriptions refused (`SUBACK 0x80`). Retained publishes are forwarded as regular records (no last-value-wins). QoS 0/1/2 all deliver at-least-once end-to-end. Unmapped topics close the connection without `PUBACK` (fail-closed).
  - Tail-file reference connector + checkpoint runtime (`FileCheckpointStore`, atomic rename, `0600`) proves the source-connector contract without external dependencies.
- **Not built (dependency-gated):** Kafka/Redpanda/NATS sources, SQL CDC (Postgres logical replication) and NoSQL change-stream connectors. They need third-party drivers, which this repo does not add ad hoc; each is named in plan 007 and tracked in ADR-013. MQTT 5.0 native, SUBSCRIBE support, retained-store, and topic-checkpoint coordination for multi-instance deployments are also unbuilt.
- Checkpointing is **file-based by default**; a `__integration_checkpoints`-backed store was deferred (documented in plan 007 slice 1).
- Everything speaks to Firn through `sdk/firngo`; no storage-path or replication changes; storage invariants untouched.
