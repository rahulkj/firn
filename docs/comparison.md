# Firn vs. the field — messaging & streaming comparison (2026-11-13)

Scope: Firn v0.5.0 (this repo) compared against Apache Kafka, Redpanda, Apache Pulsar, RabbitMQ, NATS JetStream, Redis Streams, AWS Kinesis Data Streams, Google Pub/Sub, and AWS SQS.

Honesty box, read this first:

- **This document is written by the project.** It is as objective as we could make it, but self-assessment bias is real; treat every "Firn wins" row as a claim to verify, not a conclusion.
- **Firn is v0.5.0** from a single maintainer, with no production deployments outside this repo, a GA-but-single-region cluster mode, laptop-scale benchmarks (~350–500 msg/s durable produce by default — or ~186,000 msg/s with `-flush-window=2ms` — measured 2026-11-13/14 on an Apple M3 Pro), and a preview federation feature. Competitors below are million-hours-in-production systems. If you need a broker to run the business by, buy or self-host Kafka/Redpanda/Pulsar — or use a managed service.
- Competitor facts below are from their public documentation as of 2026-11-13. Performance numbers for competitors are **vendor/community claims**, not independent measurements. Firn's numbers are measured; theirs are cited. Treat both accordingly.
- Azure Event Hubs / Azure Service Bus are not included (insufficient verified data in this pass); they are comparable to Kinesis/SQS in the managed tier.

## TL;DR verdict

Firn is **not** superior as a general-purpose production broker, and nothing here says otherwise. It is meaningfully differentiated in a few narrow places:

1. **Stream + queue semantics on one append-only log** with broker-native DLQ (`<queue>.dlq` after N attempts) and one replication/audit path — the cleanest expression of "task queue and event log in one system" among OSS brokers. (With one self-flagged defect: the commit path is O(N²) today — see "Defects found by our own benchmark pass".)
2. **Byte-offset-preserving passive federation with a fenced cutover** — a first-class DR story no mainstream broker ships out of the box (Kafka/Pulsar need offset translation via tooling). Preview-quality, unproven at scale.
3. **Secure-by-default in the binary, with built-in audit** — fail-closed bootstrap token, hashed keys, roles, per-key + per-IP rate limits, mesh mTLS, HTTP hardening headers, append-only audit in every mode. OSS Kafka/Pulsar/RabbitMQ/NATS make you assemble this yourself (or buy Confluent/Redpanda Enterprise).
4. **Readable, MIT-licensed, ~12k-LOC single binary** — the learning/embedded/teaching/edge niche. Ties with NATS and Redpanda on "one binary", beats both on code size and license permissiveness.

In every "boring" axis — throughput, exactly-once, ecosystem, maturity, multi-tenancy, geo-replication at scale — Firn loses, mostly by a lot.

## Defects found by our own benchmark pass (2026-11-13)

Our own tooling surfaced these; they are stated here because a comparison is worthless if it hides them. None are fixed in this document.

- **Commit path is O(N²).** `internal/groups/offset_store.go`: `Coordinator.CommitOffset` calls `Get`, which triggers `refreshIfMoved`; each commit moved the high watermark, so **every commit re-folds the whole `__consumer_offsets` log**. The queue ack path commits per contiguous ack, so commit cost grows quadratically over a queue's life. Profiling a queue bench: `groups.fold` = 53% of CPU, `MemLog.Fetch` = 30%. Not fixed — offsets/internal topics are storage-adjacent and need human review.
- **Queue path pays two fsyncs per message** (attempt record on claim + per-ack offset commit). At the durable produce ceiling (~370 msg/s), sustained queue throughput is a few hundred messages/s.
- **Parallel produce does not scale.** Durable fan-out across 3 partitions from multiple goroutines: ~374 msg/s aggregate vs ~370 single-threaded — the device plus per-partition fsync serialization bound the node.
- **Client-side default cap.** Authenticated clients are limited to the default `-rate=100` ops/s per API key (and `-anon-rate=20/min` anonymous). Plan capacity accordingly or pass `-rate` explicitly; broker-side saturation queues without dropping.
- **Test-only fix shipped in the same pass:** `bench/fetch_bench_test.go` wrapped fetch offsets against the global seed instead of the per-partition watermark (panic at large `-benchtime`). Test-only; no storage semantics changed.

## Feature matrix

Marks: ✅ full · ◐ partial/scoped · ❌ no · 💰 paid-tier only · — n/a · † = defect/limitation found by our own 2026-11-13 bench pass — see "Defects found by our own benchmark pass" above

### Self-hosted / open source

| Capability | Firn v0.5.0 | Apache Kafka 4.x | Redpanda | Apache Pulsar | RabbitMQ | NATS JetStream | Redis Streams |
|---|---|---|---|---|---|---|---|
| License | MIT | Apache-2.0 | BSL (core), Enterprise paid | Apache-2.0 | MPL | Apache-2.0 | Tri-license incl. AGPLv3 (Redis 8) |
| Single static binary, no external deps | ✅ | ❌ (JVM; KRaft built in — no ZK, but JVM + config) | ✅ (C++, no JVM/ZK) | ❌ (brokers + BookKeeper + ZK metadata, config-heavy) | ◐ (Erlang VM) | ✅ (Go) | ❌ (needs Redis server; often paired with Sentinel/Cluster) |
| Streaming (append, offsets, replay) | ✅ | ✅ | ✅ (Kafka API) | ✅ | ◐ (Streams engine, separate semantics from quorum queues) | ✅ | ◐ (XADD/XRANGE; replay yes, consumer groups yes) |
| Task-queue semantics (ack, redelivery, DLQ) | ✅ first-class (`mode: queue`) † | ❌ (app-level / connectors) | ❌ (app-level / connectors) | ✅ (subscriptions: exclusive/shared/key_shared; DLT supported) | ✅ first-class (quorum queues + DLX/dead-letter routing) | ✅ (consumers ack; advisory/term policies) | ◐ (XACK/XCLAIM + PEL; no broker DLQ primitive) |
| Stream + queue on the same log | ✅ one engine | ❌ separate concerns | ❌ separate concerns | ◐ (topics + subscriptions; one storage layer, multiple subscription modes) | ◐ (queues and streams are two engines in one broker) | ◐ (streams + JetStream consumers) | ◐ (one data structure, weaker guarantees) |
| Broker-native DLQ with redrive | ✅ (`<queue>.dlq`, auto-created, CLI redrive) † | ❌ app-level | ❌ app-level | ✅ (DLT, you wire consumers) | ✅ (dead-letter exchanges, battle-tested) | ◐ (retry + advisory; not a first-class DLQ) | ❌ app-level (XCLAIM + hand-rolled) |
| Consumer groups / assignment | ✅ range, roundrobin | ✅ range/roundrobin/sticky/ cooperative-sticky | ✅ same as Kafka | ✅ (subscriptions, failover/shared) | ✅ (competing consumers on queues) | ✅ (durable/ephemeral, pull/push) | ✅ (XGROUP) |
| Ordering | per partition | per partition | per partition | per key (key_shared) / per partition | per queue (quorum queues) | per subject/stream config | per stream |
| Delivery guarantee | at-least-once | exactly-once (EOS, transactions KIP-98) | exactly-once (Kafka EOS-compatible) | effectively-once (broker dedup + ack) | at-least-once (quorum queues) | at-least-once + dedup window ≈ effectively-once publish | at-least-once |
| Transactions / atomic multi-partition write | ❌ | ✅ | ✅ | ◐ (broker dedup, not full txns) | ❌ (no cross-queue tx) | ❌ | ◐ (MULTI/EXEC, watch) |
| Replication / HA | ◐ 3-node cluster, embedded Raft, **experimental**; fixed membership | ✅ ISR replication, KRaft, mature, tens of thousands of nodes in prod | ✅ Raft-ish replication, no ZK | ✅ BookKeeper quorum replication, mature | ✅ quorum queues (Raft), mature | ✅ Raft replication (R1/R3/R5) | ◐ async primary/replica; Cluster sync writes optional, weaker |
| Geo-replication / DR | ◐ **passive byte-identical, offset-preserving, fenced cutover** + active-active (preview); unproven at scale | ◐ MirrorMaker 2 / Replicator (offsets translated, not preserved); or Confluent Cluster Linking 💰 | ◐ Confluent interoperability / tiered storage; no offset-preserved fenced cutover | ✅ **native geo-replication (data, not subscriptions)**, mature, multi-DC active-active | ◐ plugin/federation features exist; not offset-safe | ✅ superclusters/leaf nodes, geo-distributed | ◐ (async replicas; not offset-consistent) |
| Multi-tenancy | ❌ (none) | ◐ quotas only | 💰 SSO/RBAC | ✅ tenants/namespaces (real isolation) | ◐ vhosts + tags + limits (weaker) | ✅ accounts + JWT/NKey | ❌ (ACL users only) |
| Tiered storage / infinite retention | ❌ (whole-segment retention only) | ✅ KIP-405 (tiered storage) or 💰 | 💰 | ✅ native (S3/GCS/Blob offload) | ❌ (TTL + maxLength only) | ◐ (max_age/max_bytes; no cloud-tier offload) | ◐ (MAXLEN trim) |
| Ecosystem (Connect, Streams API, Schema Registry) | ❌ (Go SDK only; no Connect/Streams/SR) | ✅ Connect + Streams + Schema Registry (Community limited; Confluent SR) + huge tooling | ✅ Kafka-compatible (SR, HTTP proxy, consoles) | ◐ (functions, IO connectors, SR via Pulsar; smaller) | ◐ (huge plugin ecosystem; AMQP tooling) | ◐ (KV/ObjStore; Go/Rust/etc clients; weaker Kafka-interop) | ◐ (every language has a Redis client; no Kafka interop) |
| Wire compatibility | ❌ (bespoke gRPC + HTTP) | ✅ (the wire protocol) | ✅ (Kafka API) | ❌ (bespoke) | ◐ (AMQP 0-9-1 + Streams protocol; AMQP 1.0) | ◐ ("nats://" + JetStream API; no Kafka compat) | ❌ (Redis protocol) |
| Admin UI / ops tooling | ◐ built-in read-only UI (`/ui/`) | ◐ CLI; UI via Confluent/other | ✅ Console (part of source-available core) | ◐ Prometheus + dashboards; UI via third parties | ✅ management UI + metrics + alarms | ◐ monitor tooling; no official full UI | ◐ Redis Insight |
| Security defaults out of the box | ✅ bootstrap token, roles, mTLS mesh, audit always-on | ❌ (TLS/SASL/ACLs configured separately; no audit in Apache build) | ◐ auth off by default historically; audit = 💰 | ❌ (TLS/JWT you wire up; audit via contrib) | ◐ (TLS + plain/SCRAM you configure) | ◐ (TLS + NKeys; no audit) | ❌ (no auth if bound open; ACL since 6+) |
| Audit log | ✅ built-in, append-only, every mode | ❌ 💰 (Confluent Platform) | ❌ 💰 (Enterprise) | ❌ contrib/OSS mirage | ◐ event emitters / management events | ❌ | ❌ |
| Persisted state format / recoverability | ✅ CRC frames, torn-tail truncation, `kill -9` tested | ✅ battle-tested, snapshots + ISR | ✅ battle-tested | ✅ BookKeeper, battle-tested | ✅ battle-tested | ✅ battle-tested | ◐ (forks vary; RDB/AOF) |
| Throughput (claimed, same class of hw) | ~350–500 msg/s produce (measured 2026-11-13, fsync/append, M3 Pro; no partition scaling) † | ~1M+ msg/s/broker (vendor/community) | ~1M+ msg/s/broker (vendor, C++) | ~1M+ msg/s/cluster (vendor) | ~50–100k msg/s/node (community) | ~100k–1M+ msg/s (vendor, small payloads) | ~100k–1M ops/s (memory-bound) |
| Operational complexity | **low** (one binary) | **high** (JVM, ISR, KRaft, partitions/ISR tuning) | low–medium (no JVM, but cluster ops) | **high** (brokers + bookies + ZK + tenants) | medium (Erlang, vhosts, quorum tuning) | low–medium | low (if Redis already run) |
| Production maturity / community | ❌ zero users outside repo; v0.5.0 | ✅ massive, CNCF-adjacent, decades | ◐ real users, large community | ◐ real users (Yahoo/WeChat/etc.) | ✅ massive, decades | ✅ real users (many cloud-native stacks) | ✅ massive (as a database) |

### Cloud-managed (serverless / managed)

| Capability | AWS Kinesis Data Streams | Google Pub/Sub | AWS SQS (+SNS) |
|---|---|---|---|
| Model | Shard-based stream, pull | Global topics+subs, push/pull | Managed queue (standard + FIFO) |
| Exactly-once | at-least-once | ✅ exactly-once delivery GA (pull subs, regional) | FIFO: exactly-once processing (dedup id, ≤300/s, 3000/s batched / high-throughput mode) |
| Ordering | per shard | per ordering key (optional) | FIFO per message group |
| DLQ | app-level (pair with SQS) | ✅ DLT supported | ✅ native redrive to DLQ |
| Retention | 24h default → 365d max | 7d default → 31d max | 14d max |
| Scale | shard math (1 MiB/s or 1k rec/s per shard; on-demand mode in most regions) | ✅ effectively unbounded, global | standard ~unlimited; FIFO ≤300/s per API action, 3,000/s with batching; higher in high-throughput mode in supported regions |
| Ops burden | none (shards/resharding is the main knob) | none | none |
| Cost shape | shard-hours + PUT payload units (can hurt at scale) | per-message + egress (expensive at very high volume) | per-request (cheap at small/medium; free tier generous) |
| Ecosystem | KCL/KPL, Lambda, Firehose, Flink | Beam/Dataflow, global replication | everything AWS; SNS fan-out |
| Fits | AWS-native streaming, Lambda consumers | GCP-native global messaging | AWS-native decoupling, simplest possible |
| Multi-tenancy/audit | IAM + CloudTrail (not per-message audit) | IAM + Cloud Logging | IAM + CloudTrail |

## Category scorecard — who is better at what

| Category | Winner | Runner-up | Why |
|---|---|---|---|
| Raw sustained throughput at scale | **Redpanda** (then Kafka) | Kafka | C++/no-JVM; Kafka's ISR + JVM tuned for decades; both are "a million msg/s class" brokers. Firn is ~three orders of magnitude behind. |
| Lowest-effort, zero-ops messaging | **SQS** | Pub/Sub | One API call, SLA'd, no capacity planning. Firn loses; running a broker is ops. |
| Exactly-once / EOS | **Kafka** (EOS + transactions) | Pub/Sub (EOD), Pulsar (dedup) | Kafka is the reference implementation. Firn: none. |
| Task-queue work (ack/retry/DLQ) at production scale | **RabbitMQ** | SQS | DLX, per-message ack, priorities, plugins, management — done to death, highly reliable. Firn ties on semantics, loses on hardening and on commit-path scaling (see defect log †). |
| Event streaming + replay (log as source of truth) | **Kafka** | Pulsar | Retention, ISR, Connect, Streams, tooling moat. Firn ships the log, none of the moat. |
| Geo-replication, active-active, multi-DC | **Pulsar** | NATS (superclusters), Kafka (MM2/Replicator w/ offset translation) | Native, topology-aware, battle-tested at Yahoo/StreamNative scale. Firn: preview, unproven. |
| Multi-tenancy isolation | **Pulsar** (tenants/namespaces) | NATS (accounts), RabbitMQ (vhosts) | Pulsar's tenancy is a real isolation model. Firn: none. |
| Kafka API compatibility | **Redpanda** | Kafka (obviously) | Firn: none — you can't point Kafka clients at Firn. |
| Built-in audit + secure-by-default (OSS) | **Firn** | — (OSS alternatives charge: Confluent/Redpanda Enterprise) | Everything else in OSS assembles TLS+SASL+ACLs by hand and ships no audit. Honest caveat: Firn's audit is real but its GA story isn't. |
| DR with offset-preserving cutover, out of the box | **Firn** (preview) | — (Pulsar geo loses offsets; MM2 translates; Confluent 💰) | Byte-identical passive federation + fenced sink + cutover runbook. Genuinely unique-looking. Unproven at scale — treat as interesting, not bankable. |
| Single append-only log, stream **and** queue, one binary | **Firn** | RabbitMQ (queues+streams, two engines), Pulsar (one storage layer, shared subs semantics) | Firn's cleanest win. One storage path, one replication path, one DLQ primitive, one audit trail. |
| Edge / IoT / embedded / teaching | **NATS JetStream** | Firn | NATS is tiny, everywhere, huge client list, proven at edge. Firn is smaller and MIT, but zero community. |
| Managed streaming on GCP | **Pub/Sub** | — | Serverless, global, EOD, no shards. Firn is self-host only. |
| Managed streaming on AWS | **Kinesis** (streaming) / **SQS** (queues) | — | Native IAM, Lambda, Firehose. Firn is self-host only. |
| "Just use what we have" (already on Redis) | **Redis Streams** | — | Zero new infra; decent groups/acks; weaker durability & no DR story. |
| Cost at very large scale (self-host) | **Kafka / Redpanda** | Pulsar | Commodity hardware, one disk per broker, tiered storage locks in retention. Firn can't hold this line. |
| Cost at small/medium scale (no ops team) | **SQS / Pub/Sub** | — | Per-message pricing beats running anything. |
| License freedom (MIT/Apache) | **Firn** (MIT) | Kafka/Pulsar/NATS (Apache-2.0) | Redpanda BSL, Redis tri-license, RabbitMQ MPL. Firn tops this, trivially. |
| Enterprise-friendly source, small team hackable in a weekend | **Firn** | NATS (bigger codebase) | ~12k LOC; storage invariants documented; readable by one person. |
| Cluster DR cut-over runbook included | **Firn** (preview) | — | `docs/federation-cutover.md` ships in the box; no equivalent OSS runbook for Kafka DR as fleshed. Unproven. |
| Ops burden to first working broker | **Firn** / NATS | Redpanda | `go run ./cmd/firnd` → `/healthz`. Kafka: a day. Pulsar: a week. |

## Where Firn is genuinely differentiated (and the catch for each)

1. **One log, both semantics, DLQ in the box.** Produce once; same record is replayable (stream consumer) and pull-able (queue consumer). `<queue>.dlq` auto-creates after 3 delivery attempts; `firn-cli dlq redrive` re-produces into the source queue. DLX-style routing, no second system, no connector.
   *Catch:* RabbitMQ + quorum queues + DLX covers this with 20 years of hardening; Pub/Sub/SQS give you DLT/DLQ managed. Firn's win is architectural elegance, not features — and its commit path is O(N²) today (defect log †).
2. **Offset-preserving passive DR federation.** Byte-identical replication, offsets kept, sink read-only until a human cuts over; cursors and lag are first-class (`/ui/federation`, `firn_federation_cursor_lag_seconds`); cutover runbook ships in the repo.
   *Catch:* federation is preview; no external users; active-active is lossy-ish by design (fresh offsets by necessity); Pulsar achieves multi-DC DR without pretending byte-identical H/W folklore.
3. **Secure-by-default, audit always-on.** Zero-key broker serves `/healthz` only; one-time bootstrap token on stderr; keys hashed at rest; roles and per-key/IP rate limits; mesh traffic is mTLS-only; HTTP hardening headers; `__audit` appended in every mode including dev.
   *Catch:* you trade features for this — no exactly-once, no big-throughput tuning, no plugin ecosystem. And "secure by default" is not the same as "audited by an external party".
4. **MIT + ~12k LOC + docs culture.** The whole thing fits in your head; ADRs explain storage invariants; `kill -9` fault-injection is in the test suite.
   *Catch:* zero external contributors; bus factor = 1; not battle-tested.
5. **Durability-first stance, explicit and documented.** fsync-per-append is kept deliberately; the 50k msg/s produce bar was retired by ADR-005 amendment rather than faked. ~350–500 msg/s is the measured expectation (2026-11-13).
   *Catch:* everything else on this page ships configurable durability; only Firn made it non-configurable and paid for it with throughput.
6. **Queue DLQ + broker-native redrive.** Redrive is a CLI one-liner; no external tooling.
   *Catch:* SQS `redrive` policy is a checkbox; RabbitMQ has DLX with TTL/DLX chaining and plugins.

## Where Firn loses, plainly

- **Throughput**: ~350–500 msg/s produce (fsync, measured; no partition scaling) vs ~1M msg/s class for Kafka/Redpanda/NATS. Not a rounding error; a strategy.
- **Commit path**: O(N²) in `__consumer_offsets` — every commit re-folds the internal offsets log (defect log †). Queue acks commit per message; sustained queue throughput degrades as the log grows.
- **Exactly-once / transactions**: none. Kafka EOS, Pulsar dedup, Pub/Sub EOD and SQS FIFO all beat it for financial/dedup-critical flows.
- **Cluster mode**: experimental, static membership, leader-affinity re-grants, unsealed-tail at-risk window, no rolling upgrade (restart window required).
- **Ecosystem**: no Connect, no Streams API, no Schema Registry, no Kafka wire protocol — Kafka-compatible clients can't talk to Firn at all.
- **Federation**: preview; active-active re-origins offsets (by design); passive mode is the only offset-preserving one; cursors/replication unproven under partition/loss; no dynamic membership.
- **Multi-tenancy / quotas**: none. Pulsar and NATS have real tenant models; Kafka has quotas.
- **Retention/tiering**: whole-segment deletion only; no tiered storage, no compaction for user topics (internal `__` topics compact only at clean shutdown).
- **Ops tooling**: read-only UI and decent metrics, but no control plane, no cluster-wide rebalance tooling, no partition re-assignment, no broker draining.
- **Maturity**: zero production hours outside this repo; v0.5.0; single maintainer; docs are unusually thorough but that is not the same as battle-tested.
- **Multi-AZ/quorum semantics for streamed data at scale**: experimental; Kafka/Pulsar/RabbitMQ are done here.

## When you'd actually pick Firn

- Internal service mesh / control plane where you want a tiny, hackable, MIT broker with both stream and queue semantics, and a few hundred durable msg/s is plenty (config distribution, job dispatch, audit fan-out inside a company).
- An educational or reference implementation of "how do brokers work" — the storage invariants are written down and tested.
- A DR/standby pilot for a small self-hosted Kafka where byte-identical restore has real value and you can tolerate preview semantics.
- A single-node, low-throughput, durability-paranoid work queue where you want the DLQ/audit built in and don't want to run RabbitMQ/Kafka.
- Embedded/edge scenarios where a ~14 MB single static Go binary and `go run` beats Erlang/JVM toolchains.

For essentially every other scenario, pick the incumbents:

- **AWS-native**: SQS + SNS (queues), Kinesis (streams).
- **GCP-native**: Pub/Sub.
- **Max throughput / Kafka API**: Redpanda or Kafka.
- **Multi-tenant platform as a service**: Pulsar.
- **Reliable enterprise task queue**: RabbitMQ.
- **Lightweight cloud-native messaging + KV**: NATS JetStream.
- **Already running Redis**: Redis Streams (accept the durability tradeoffs).
- **Exactly-once end-to-end**: Kafka with transactions.

## What would change this verdict

- A flush-window/group-commit ADR with property tests → 50–500x produce throughput, re-opens the throughput column.
- Fixing the `__consumer_offsets` per-commit re-fold (defect log †) → queue acks stop degrading quadratically; sustained queue throughput becomes fsync-bound, not commit-bound. A small, well-scoped internal change gated on human review.
- GA cluster mode (dynamic membership, rolling upgrades, multi-AZ rebalance) → Kafka-class HA story.
- A Kafka wire-protocol shim or first-class Schema Registry → ecosystem moat becomes addressable.
- One external production deployment with published numbers → maturity column stops reading "zero".
- Benchmarks on cloud storage (gp3/EFS-class), not only Apple NVMe.

## Sources & verification notes

- Firn: this repo at `v0.5.0` — `CHANGELOG.md`, `docs/adr/003-storage.md`, `docs/adr/005-mvp.md`, `bench/baseline.md` (incl. the 2026-11-13 capacity re-run), `bench/capacity_bench_test.go`, `docs/plans/005-security.md`, `docs/adr/010-federation.md`, `docs/federation-cutover.md`. Measured 2026-11-13: ~370 msg/s durable produce (~2.7 ms/op), ~374 msg/s durable produce aggregated over 3 partitions with parallel writers (no scaling), ~4.0M msg/s in-memory produce, ~1.05M records/s fetch (128-record batch), ~44k deliveries/s queue software path at N=1,000 (commit-path re-fold visible at larger N). Non-generated Go ≈ 12k LOC (`find . -name '*.go' | xargs wc -l`); binary ≈ 14 MB (`-trimpath -ldflags "-s -w"`). `bench/fetch_bench_test.go` latent offset bug fixed in the same pass (test-only). Self-found defect disclosed rather than benchmarked away: `__consumer_offsets` O(N²) commit re-fold — see "Defects found by our own benchmark pass".
- Kafka: Apache Kafka docs (4.x, KRaft-only); KIP-98 (transactions/EOS); KIP-405 (tiered storage); community throughput figures are vendor/community claims.
- Redpanda: redpanda.com/docs — Community Edition under BSL; Enterprise adds SSO/audit/tiered storage; Kafka-API compatible.
- Pulsar: pulsar.apache.org — native geo-replication (data only, not subscriptions), tenants/namespaces, tiered storage (S3/GCS/Azure); StreamNative distribution.
- RabbitMQ: rabbitmq.com/docs/quorum-queues — quorum queues (Raft), DLX dead-lettering, streams engine, management UI; MPL.
- NATS: docs.nats.io — JetStream at-least-once, BYOI dedup (effectively-once publish), KV/Object Store, superclusters/leaf nodes, accounts.
- Redis: redis.io/legal/licenses — Redis 8 tri-license incl. AGPLv3; streams are at-least-once, XACK/XCLAIM + PEL.
- AWS: docs.aws.amazon.com — Kinesis Data Streams (retention 24h→365d, 1 MiB/s or 1k rec/s/shard); SQS (standard ~unlimited, FIFO 300/s → 3000/s batched; max retention 14d; DLQ redrive policy).
- GCP: cloud.google.com/pubsub — exactly-once delivery GA (pull subscriptions, region-scoped), 7–31d retention, ordering keys, dead-letter topics, global by default.
- Dates: verified 2026-11-13. Licenses and cloud service limits change; re-check before relying on any single row.
