# ADR-007: Scaled-out deployment — the block log, not a Kafka/RabbitMQ clone

Status: accepted · 2026-11-13 · Implementation proceeds via `docs/plans/002-ha.md` (phase A shipped, pending storage review); each slice lands with its own storage review gate.

## Why not "Kafka, but RabbitMQ" or "RabbitMQ, but Kafka"

Both incumbents hardwire one geometry and inherit its failure model:

| Pain | Kafka | RabbitMQ |
|---|---|---|
| Consensus placement | per partition: ISR across brokers + controller quorum | per queue: one Raft group per queue — cost scales with queue count |
| Durability unit | record batch; HWM/LEO dance per partition | message ack vector |
| Membership | partition reassignment on every broker change — hours of catch-up movement | mirrored quorum replication per queue |
| Client | fat SDK; routing embedded in client | thin; broker pushes |
| Semantics | streaming only (queue glued on with groups) | queueing only (streaming bolted on later) |
| Retention/compaction | segment deletion + log compaction rewrites | destructive per-message ack |

Cloning either means inheriting both sets of scars. The goal instead: **performance and fault isolation are products of geometry, not of tuning** — pick a geometry that is cheap to run, cheap to fail over, cheap to extend.

## Decision summary

A **block log**: the only consensus is a tiny quorum over **sealed block descriptors** (a few KB each, one per ~64 MiB or per 250 ms of writes). Records flow on a streaming data plane that never enters consensus. Reads and writes are routed by a lease on the *current* write window, not by a permanent partition "leader". Streams and queues are two semantics over the same blocks.

Three ideas carried from v1 forward unchanged: segmented CRC-framed log, int64 monotonic offsets (invariant 1), fetch-never-past-HWM (invariant 2). One thing changes: the unit of **commit** becomes the block.

## Goals / non-goals

**Goals:** add capacity by adding a node (no reassignments); failover in seconds; consensus cost decoupled from message rate; one storage engine for streams and queues; single-node embedded mode preserved for dev/eval; extendable without new subsystems.

**Non-goals (v-next):** multi-datacenter; exactly-once (still); tiered storage (block immutability makes it trivial later — noted, not designed); wire-protocol compatibility; changing v1 stream/queue semantics.

## The block log

1. **Block is the atom.** Appends accumulate in the active (unsealed) block. On size (default 64 MiB) or timer (default 250 ms) the block is **sealed** and described by a **block descriptor**:
   `{block_id = H(block), first_offset, last_offset, crcset, replica_addrs, size}`.
   Sealed blocks are immutable — never recompacted in place, never truncated, never edited. Compaction = merge blocks into new blocks (later slice).
2. **Consensus on descriptors only.** The 3-node metadata quorum (same Raft-family quorum as v-next control plane) commits descriptors. Nothing else crosses the quorum: no records, no per-batch watermarks. Consensus rate ≈ writes/s ÷ block size — e.g. 100 MiB/s of traffic seals ~1.5 blocks/s.
3. **Streaming replication.** A replica set per block (default 3) receives the block bytes over `BlockFetch` — sequential, zero-copy, ack-bytes flow; the sealing node advances a block-level HWM when a quorum of replicas holds the block's CRC-matching bytes. This replaces the record-level HWM/LEO/ISR machinery with two small counters per block: bytes received, bytes confirmed.
4. **Block = durability and recovery unit.**
   - RPO = unsealed tail only (bounded by the seal timer, default ≤ 250 ms of acknowledged writes if sealing node dies before replication).
   - Recovery = "do the sealed blocks exist and verify?" — no torn-tail analysis of committed data; unsealed bytes are simply uncommitted and invisible.
5. **GC by refcount.** Retention = unreferenced-block deletion; no segment-scan, no CRC sweep, no horizon bookkeeping complexity beyond a block refcount in the descriptor log. (v1's `RetainOverBytes/RetainOlderThan` becomes trivial and safe.)
6. **Blocks are content-addressed** (`block_id = H(content)`) — dedupe-friendly, self-verifying (any copy is checkable by hash), and the primitive a future tier/object-store archive plugs into.

## Lease tails — no partition "leader"

1. Write access to a topic partition's active block is held as a **short write lease** (default 5 s) by the node currently appending it. Lease is granted/refreshed by the metadata quorum, fenced by an **epoch** the quorum increments on every transfer.
2. **Epoch fencing everywhere** (same mechanism as a Kafka leader epoch, but for a lease): all data-plane traffic carries `(block_window, epoch)`; a node with a stale epoch cannot seal, append, or acknowledge anything. The split-brain window is bounded by lease TTL + epoch CAS — no fencing keys, no external locks.
3. **Failover:** lease holder dies → quorum notices (heartbeat timeout) → grants lease to a replica that already holds the largest sealed-block prefix → it unseals the failed tail from its own copy (it may re-append bytes it already has — block content hashing makes re-sealing idempotent). Consumers never see a gap. Expected MTTR: seconds, with **zero topic/partition reassignment and zero data movement**.
4. **Read routing** rides the same lease epoch: `GetMetadata` returns descriptor-anchored addressing (`topic → [block descriptors]`) plus the current lease holder hint; any broker holding a committed block copy serves it (blocks are symmetric — no read-leader concept at all). Merged with the client-bootstrap list from before.
5. Why a lease and not full Raft-per-topic or Bookie-style ensemble: a lease gives one writer at a time (preserves offset monotonicity and idempotent seals) at 1/100th the consensus cost of per-block Raft.

## Topology

- ≥ 3 nodes; each runs a broker, and 3 of the N run an embedded metadata-quorum **voter** (same binary, distinct role). Every node **replicates** the metadata log; only the 3 voters advance it. Production separates voters by host/VM for blast-radius, not by protocol.
- **No ZK, no etcd, no Bookie ensemble.** The metadata quorum is 3 of the N brokers themselves. Small clusters (dev, edge, single-tenant appliance) run embedded single-node metadata (today's exact mode, untouched semantics — the v1 binary remains the 1-node case of this design).

## Data plane: what the hot path looks like

```
producer ──produce──▶ lease holder ──append──▶ active block (local)
                                          │
                                          └── BlockFetch stream ──▶ replicas (byte acks)
                                          │
             block seals (size|timer) ◀────┘
                  │
                  ▼
      metadata quorum commits BlockDescriptor{...} (1 CAS)
      → HWM = block boundary, ack delivered (acks=all default)
      → block immutable; replica set retains it; lease continues or moves on
```

No record ever waits on consensus. No consensus path reads records. Queue mode rides the same loop: Pull = read + lease a delivery token; Ack = refcount/dead-letter on the same blocks' acked prefix.

## Client connection handling

- Client-bootstrapping works as on any broker: every node replicates the metadata log, so any node answers `GetMetadata` (the 3 voters arbitrate lease grants; followers serve committed blocks directly). No controller-redirect hop for reads.
- Client-side routing to the lease holder for writes; direct reads from any replica (client picks by RR/latency). On stale routing (NOT current-lease / epoch mismatch), refresh and retry — identical pattern to v1's design, same error surface.
- Connections: pool per broker, mTLS, keepalive, jittered reconnect on failover. No new client asset beyond the existing thin Go SDK; no routing proxy in the hot path.
- Connections: pool per broker, mTLS, keepalive, jittered reconnect on failover. No new client asset beyond the existing thin Go SDK; no routing proxy in the hot path.

## Consumer impact for business continuity

- Committed offsets, group state, queue attempts: unchanged semantics; replication is at block level, so group-related internal topics are ordinary block-logged topics — offset monotonicity (invariant 4) holds on both sides of a failover.
- Failover window = lease expiry + reopen tail (seconds). During it: committed reads continue (every replica is read-servable), producers see `NOT_CURRENT_LEASE` and retry — no data motion, no rebalance, no re-syncing days of backlog.
- Queue redelivery/DLQ: unchanged (attempts topic replicated; pending deliveries re-visible on node loss, attempts preserved — at-least-once, same as single-node).
- Members of a consumer group rejoin by heartbeat as today; because failover does not move partitions, the rebalance usually *doesn't happen at all* — the killer app for gRPC-streaming consumers.
- Bottom line: business continuity properties of single-node v1 are preserved, and the multi-node failure domain replaces the single point of failure.

## Failure modes (explicit)

| Failure | Writes | Reads | Notes |
|---|---|---|---|
| Node lost | lease moves in ~seconds | all committed blocks served from 2 survivors | no reassignment; no catch-up |
| 2 of 3 nodes lost | **Stall** (no quorum for new seals) | committed blocks served from survivor | fail-closed; restore one node |
| Quorum lost (2 nodes) | **Stall** | committed blocks served | metadata log replay on restore |
| Lease holder network-partitioned from replicas | unsealed window re-replicated on fencing | unchanged | epoch CAS kills the zombie |
| Block CRC mismatch on a replica | block re-fetched from content hash | served from any good copy | corrupt bytes never acknowledged |
| Quorum + all replicas of a block lost | **unavailable for that block range** | unavailable for that range | content-addressed restore from any surviving copy/tier |

## Security — API keys + RBAC UI

Unchanged in shape, simplified by symmetry:

- Every broker enforces **mTLS** internally and **TLS** for clients; API-key authN (opaque keys, hashed in the metadata log, revocable within the refresh interval) enforced per request at the broker edge — there is no leader to add to auth checks, which makes authorization uniform and cacheable.
- Roles → permission set; permissions resource-scoped (`topic:read`, `topic:write`, `queue:ack`, `dlq:manage`, `group:reset`, `admin:cluster`); group/topic tools in `eventing-cli` and the UI carry the same permissions.
- **Admin UI** ships in the broker binary; server-rendered HTML (stdlib `html/template`) hitting the same JSON admin API the CLI uses. Roles: `viewer`, `operator`, `admin`. Capabilities: cluster/blocks overview (HWM, seal rate, lease state), topic browser, consumer groups (lag, offsets), queue/DLQ inspector + redrive, key lifecycle, audit log. No new dependencies.
- Audit: append-only internal `__audit` block-logged topic, replicated, ≥ 90-day retention default.
- Rate limiting per API key at the edge; circuit breaker to protect data plane.

## Extensibility — what this geometry makes cheap

- **Compaction** = deterministic block merge (`n` blocks in → `1` out, same offsets range); testable byte-for-byte.
- **Tiered storage** = copy sealed blocks to object storage; delete local refcount. No format change, no translation.
- **Cross-cluster replication** = block copy by descriptor set; offset space is preserved end-to-end (blocks carry absolute offsets).
- **Redrive/replay tooling** = replay a block range with a new consumer token — free feature from immutability.
- **New semantics** (CDC, KV view, stream-table joins) = new *consumers of descriptors*, not new storage engines.

## Consequences

- The cost is moved, not removed: instead of 3 replicas per partition forever, you pay ~1 streaming copy per block + one 3-node quorum. Operational surface shrinks (no per-broker partition counts to manage, no preferred-leader dance, no ISR lag metrics — the metrics surface collapses to seal rate, replica lag, lease tenure).
- Honest trade-offs: tail latency is bounded by the seal timer (not by acks); the uncommitted tail is the only data at risk on a node death; block GC needs refcounting (new correctness surface — gets its own review gate); compaction ships later (first release is append-only blocks + retention).
- Loses the familiar "partition log" mental model for operators — needs good docs and a `blocks` admin view (comes free with the UI).
- This ADR unblocks: plan `002-ha` (block engine + lease + metadata quorum), plan `003-auth`, plan `004-audit`, in that order. Implementation is phased: `002-ha` starts with single-node block-mode FileLog (blocks sealed locally, no cluster yet) — a strict subset with identical client semantics — then the quorum, then replication/failover fault-injection.
- v1 is untouched until that plan lands.
