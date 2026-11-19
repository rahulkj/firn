# ADR-018: Data pipelines and transformations — field selection, mapping, and AI-assisted processing

Status: accepted · 2026-11-19 · Implementation proceeds via `docs/plans/009-data-pipelines.md`. Depends on ADR-016/ADR-017 as the ingress/egress boundary.

## Context

ADR-016 and ADR-017 introduce a model for moving data into Firn from external sources and forwarding data from selected Firn topics/queues to external systems. In practice, ingress-to-egress flows rarely move records unchanged: operators need to select a subset of fields, rename or reshape structures, normalize formats, filter records, enrich with reference data, and route different outputs to different destinations. Today, users must implement this logic in bespoke consumers/producers, duplicating effort and making behavior hard to audit and operate consistently.

There is also increasing demand to use AI/LLM capabilities for tasks that are difficult to express as static rules (for example, extracting fields from unstructured text, classifying records, summarizing payloads, or suggesting/assisting mappings). Any such capability must be optional, observable, and bounded: Firn is a correctness-focused broker, and transformation logic must not undermine delivery guarantees, security posture, or the simplicity of the core.

## Decision

### 1. Pipelines as a composable integration-layer capability

Data pipelines are defined as **declarative, managed processing flows** that run in the integration layer (outside `firnd` and storage internals, consistent with ADR-016/017). A pipeline:

- **Consumes** from one or more configured inputs: Firn topics (via consumer groups) and/or Firn queues (via pull/ack), and optionally read-only external/reference sources used for enrichment.
- **Transforms** records through an ordered set of stages (filter, project/select fields, rename, map, enrich, split/route).
- **Produces/forwards** results to one or more outputs: Firn topics/queues and/or external sinks defined by egress routes (ADR-017).

Pipelines MUST use the official client/API surfaces; they MUST NOT read or write log segment files, internal topics (except where explicitly permitted as a documented checkpoint mechanism), or bypass broker authn/authz/fencing semantics (ADR-003, ADR-007, ADR-011).

### 2. Transformation model

The v1 transformation capability is intentionally constrained and explicit:

- **Record envelope:** stages operate on a normalized envelope with at least: key (bytes/string, optional), value/payload, headers/attributes (string map), and metadata (source topic/queue, partition, offset or delivery id, timestamp where available). Original bytes MUST be preservable when a stage is configured as pass-through.
- **Field selection and projection:** users can declare which fields/paths to keep, drop, or rename from structured payloads (e.g. JSON/object-like data) and can emit a reshaped output document/value. Projection MUST be deterministic given the same input and configuration.
- **Mapping and type handling:** explicit mappings (rename, cast/format conversions where supported, constant/default values) are allowed; implicit or lossy coercions MUST be opt-in and surfaced in validation/metrics. Behavior on missing fields or type mismatches (fail record, drop field, default) MUST be configurable per stage/route.
- **Filtering and routing:** predicate-based include/exclude and content- or metadata-based routing to named outputs are supported as declarative rules. Routing decisions MUST be explainable from configuration and recorded in logs/metrics at appropriate sampling/debug levels (without logging sensitive payloads by default).
- **Multi-source consumption and correlation:** a pipeline may consume from multiple Firn sources. True stream-stream joins, windowed aggregations, and global ordering across sources are **not** provided by default in this ADR's initial scope; pipelines that combine sources do so by independent consumption plus explicit, bounded correlation/enrichment steps (e.g. lookup/enrich by key against a reference dataset/cache) with documented consistency and latency characteristics.

Unsupported or ambiguous transformations MUST fail validation at configuration time where possible; at runtime they MUST follow the configured error policy (dead-letter/quarantine) rather than producing partially correct silent output.

### 3. AI-assisted processing (optional)

Pipelines MAY include optional AI-assisted stages for tasks such as unstructured-to-structured extraction, classification, field suggestion/mapping assistance, normalization, or summarization. AI assistance is a **configurable, pluggable capability**, not a requirement for pipelines.

Constraints:

- **Opt-in and isolatable:** AI stages are disabled by default and enabled per stage/pipeline with explicit provider/configuration. Systems MUST remain fully operable using deterministic (non-AI) stages alone.
- **Provider abstraction:** AI stages call an external or separately deployed inference service through a defined interface (e.g. prompt/input schema in, structured output/outcome out). The broker core MUST NOT depend on any AI/LLM runtime or vendor SDK. Any third-party AI client/library dependency introduced by the integration runtime/connectors requires an explicit recorded decision/ADR context and human approval per repository rules.
- **Determinism and validation:** AI outputs MUST be treated as untrusted until validated against a declared schema/contract (required fields, types, allowed values). Pipelines MUST support fallback behavior on invalid/low-confidence/time-out responses (retry, route to review/DLQ, or drop only if explicitly configured) and SHOULD record model/provider identifier and version/config metadata for traceability where available.
- **Cost/latency controls:** AI stages MUST support timeouts, rate/concurrency limits, and size limits; operators must be able to bound spend and tail latency.
- **Data governance:** sending record content to an AI provider is a data-export action. Configurations MUST make the destination of data explicit; secrets/credentials MUST be handled per ADR-011; payload logging/redaction policies MUST prevent unintended disclosure.

AI-assisted mapping/suggestion tooling used at **design time** (to help an operator author projection/mapping rules) is distinct from **runtime** AI transformation: design-time assistance may be offered, but runtime behavior MUST be driven by the validated, versioned pipeline configuration that is actually deployed.

### 4. Delivery semantics and correctness

- Pipelines inherit Firn's and the integration layer's **at-least-once** baseline (ADR-016, ADR-017). Transformation stages MUST be designed so that retries/reprocessing are safe: prefer deterministic transforms and idempotent output keys/ids; where a stage has side effects or non-determinism (including AI), the pipeline MUST document the implications and provide deduplication/outcome-tracking hooks where feasible.
- **Acknowledgment/commit ordering:** input offsets/acks MUST advance only after outputs have been durably produced/forwarded according to the route's acknowledgment policy, preserving the no-silent-loss posture of ADR-016/017. On failure after partial fan-out, recovery may produce duplicates to one or more outputs unless an output provides transactional/idempotent guarantees explicitly configured and verified.
- **Error handling:** per-stage failures (parse errors, validation failures, AI errors, sink rejects) follow explicit policy: retry, dead-letter/quarantine with full error and lineage metadata (pipeline id/revision, stage, source coordinates, attempt), or fail-fast the pipeline. Silent drops are forbidden unless explicitly configured for a narrowly defined case and metered.
- Pipelines MUST NOT weaken Firn storage invariants or queue delivery semantics (ADR-003, ADR-004).

### 5. Configuration, versioning, and observability

- Pipeline definitions are declarative, versioned artifacts (schema TBD in implementation plan). Validation MUST reject unknown fields/incompatible versions and MUST verify referenced topics/queues/routes/connectors where resolvable.
- Safe rollout practices SHOULD be supported: dry-run/validate mode, disabled-by-default new revisions, and the ability to identify which revision processed a record (propagated as metadata/header where appropriate).
- Required observability: per-pipeline and per-stage throughput, latency, error/retry/DLQ counts, filter/drop counts, AI call counts/latency/error/timeout rates (when enabled), and lag/backlog relative to inputs. Tracing/lineage metadata SHOULD be sufficient to debug a record's path without requiring payload logging.

### 6. Security and isolation

- Pipelines run with least-privilege Firn identities for their declared inputs/outputs.
- Transformation runtimes SHOULD isolate untrusted parsing and external calls (including AI providers) with timeouts, resource limits, and failure containment so a bad record or provider outage cannot stall unrelated pipelines.
- User-provided expressions/code, if ever supported, are out of scope for the initial declarative model; introducing executable user code would require a separate security review and ADR due to sandboxing and supply-chain risks.

## Consequences

- Users gain a consistent, declarative way to select fields, reshape and route data across multiple sources and destinations, reducing bespoke glue and making behavior auditable.
- The integration runtime becomes a compute-bearing data path with capacity, latency, and failure modes that operators must manage (separate from `firnd`'s storage path).
- AI-assisted stages enable high-value extraction/mapping use cases but introduce external dependencies, non-determinism, cost, and data-governance obligations; these are contained by opt-in configuration, validation, and fallbacks.
- Correctness depends on careful ack/commit ordering and idempotent design; the project must provide conformance tests and clear documentation of per-stage guarantees.
- Declarative expressiveness will be limited initially; complex event processing, joins, and exactly-once pipelines remain future or out-of-scope work.

## Non-goals

- Implementing transformation, pipelines, or AI inference inside `firnd` or the storage engine.
- Exactly-once end-to-end processing across arbitrary inputs, transforms, and external outputs.
- A general-purpose stream-processing engine (windowed joins/aggregations, global stateful CEP) in the initial scope.
- Mandatory AI: no pipeline requires an AI provider to function.
- Allowing unreviewed arbitrary user code/plugins in the broker or runtime without a dedicated sandboxing ADR.
- Modifying storage invariants, replication, or federation semantics (ADR-003, ADR-007, ADR-010).

## Implementation notes (planned)

- Deliver via a plan (slices): (1) pipeline config schema, envelope model, validation, and deterministic projection/filter/map stages with conformance tests (ordering, retry, DLQ, ack/commit), (2) multi-output routing and enrichment/lookup hooks, (3) integration with ingress/egress route definitions (ADR-016/017) and observability, (4) optional AI-assisted stage interface with provider adapter, schema-validated outputs, and guardrails (timeout/rate/limit/fallback) — shipped only with an explicitly approved provider/dependency story.
- Any slice introducing executable expressions, new external runtimes, or third-party AI/connector dependencies requires human review and approval per repository rules.
- This ADR composes with ADR-016 and ADR-017 and does not amend ADR-001–015 or storage invariants.

## Implementation notes (2026-11-19): reference implementation shipped

What shipped vs the decision record, honestly:

- **`firn-pipeline` (new binary) and `internal/pipeline`.** Deterministic engine shipped: `filter` (eq/ne/in/exists/contains/gt/ge/lt/le), `project` (keep/drop/rename with dot-paths), `map` (set/default/cast string|number|bool), `enrich` (read-only JSONL reference, `match_field` join, mtime-invalidated cache, `on_miss: passthrough|dlq`), `route` (first-match, **terminal**, required when multiple outputs). Non-object JSON records and stage failures go to the pipeline DLQ; `on_error: fail` aborts the batch instead.
- **Inputs v1: exactly one** (topic or queue); multi-source correlation (independent consumption + bounded enrichment) is the documented path, and a dedicated correlation spec is unbuilt.
- **AI stage shipped, opt-in**: providers `mock` (tests) and `openai_compatible` (stdlib HTTP, no new dependencies) with contract-validated JSON output (`output_contract` types), bounded retries (`on_invalid: retry` caps at 3), per-minute rate cap (default 60), daily call budget with circuit breaker (default 10000; exhaustion ⇒ records DLQ until midnight reset or budget raise), hard timeout, HTTPS-only endpoints, env-only keys, fields merged into the record. Payloads/prompts are never logged. Removing a guardrail is a release blocker per spec.
- **Runtime**: `firn-pipeline -config … [-validate|-dry-run -sample f.jsonl]`; revision = SHA-256 of config (or `-revision`); input position advances only after all outputs are durable; unconfirmed batches re-served; input at-least-once, duplicates possible across restarts.
- **Not built:** slice 5 design-time mapping assistant, multi-source inputs, Prometheus metrics for stages, `firn-cli` integration, executable user code (requires its own sandboxing ADR).
