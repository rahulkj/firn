# Plan 001: MVP single-node broker (slices 1–6)

This is the implementation playbook for everything up to v0.1.0. It is detailed enough to drive directly with a coding agent, slice by slice — but each slice still follows the same loop:

> **(a) paste the slice spec → (b) agent implements, tests first → (c) you read the diff → (d) `go vet ./... && go test ./...` green → (e) commit with the suggested message → (f) human review for anything in `internal/log`, `internal/meta`, or offset handling before push.**

Non-negotiable rules (repeated from `AGENTS.md`):

1. Storage diffs always get human review. Green tests are necessary, not sufficient.
2. One slice per session. If a slice needs a plan change, edit this file first, then implement.
3. Write the failing/property test before the implementation for any produce/fetch/ack/redeliver change.
4. No new dependencies without an ADR and explicit approval.
5. Never `git push --force`; never skip `.gitignore`d data dirs in commits.

---

## Slice 1 — in-memory core (START HERE)

**Goal:** produce/fetch end-to-end over gRPC backed by `MemLog`, partitions, topic metadata. All in-memory; restart loses data (documented).

**Files to create/modify**

- `internal/broker/partitioner.go` — `Partition(key []byte, n int) int` using FNV-64a (`hash/fnv`); `key == nil` → round-robin counter (use `atomic.Int64`), empty key → round-robin.
- `internal/broker/broker.go` — replace `doc.go` guidance with a real type:
  ```go
  type Broker struct {
      Registry *meta.Registry
      mu       sync.RWMutex
      parts    map[string]map[int]*log.MemLog // topic -> partition -> log
      rr       atomic.Int64                    // round-robin counter
  }
  func New() *Broker
  func (b *Broker) Produce(topic string, partition *int, key, value []byte) (partitionOut int, offset int64, err error)
  func (b *Broker) Fetch(topic string, partition int, offset int64, max int) ([]log.Record, error)
  func (b *Broker) TopicPartitionCount(topic string) (int, error)
  ```
  `New()` must lazily create partition logs on first produce (and on fetch-unknown-topic return `meta.ErrTopicNotFound`).
- `internal/api/slipway.proto` — `service Slipway { rpc Produce(ProduceRequest) returns (ProduceResponse); rpc Fetch(FetchRequest) returns (FetchResponse); }` with messages `ProduceRequest{string topic; int32 partition; bytes key; bytes value}` (`partition = -1` → auto), `ProduceResponse{int32 partition; int64 offset}`, `FetchRequest{string topic; int32 partition; int64 offset; int32 max_records}`, `FetchResponse{repeated FetchRecord records}`, `FetchRecord{int64 offset; bytes key; bytes value}`.
- Codegen: use `buf` (`buf.gen.yaml` with `go_out`) or `protoc --go_out --go-grpc_out` — pick one, document in `AGENTS.md` update. Generated code in `internal/api/pb/`. If protoc tooling is unavailable in the environment, **stop and ask** rather than hand-writing protobuf stubs.
- `internal/api/grpc.go` — gRPC server implementing `pb.SlipwayServer` against `*broker.Broker`; `RegisterSlipwayServer(grpcServer, srv)`.
- `internal/api/http.go` — admin mux: `POST /topics` `{name, partitions}` (201 / 409 on duplicate / 400 on invalid), `GET /topics` (list), served alongside `/healthz`.
- `cmd/slipwayd/main.go` — wire: `broker.New()` → gRPC listener on `-grpc :9093` + HTTP admin/health on `-addr :9092`; graceful shutdown on SIGINT/SIGTERM (existing pattern).
- Tests:
  - `internal/broker/broker_test.go` — key stickiness (same key → same partition), round-robin for empty key, unknown-topic error, fetch bounds, offset sequence per partition.
  - `internal/api/grpc_test.go` — bufconn (`google.golang.org/grpc/test/bufconn`) end-to-end: produce 1000 msgs → fetch → byte-identical; fetch from unknown topic → `NotFound` status.
  - `internal/api/http_test.go` — `httptest`: create topic 201 → duplicate 409 → list contains it.

**Acceptance:** all tests pass; `curl :9092/healthz` ok; gRPC produce/fetch via `grpcurl` or a test client works; restart wipes data (expected, documented in README "Status").

**Suggested commit:** `feat(core): in-memory produce/fetch over gRPC with partition assignment`

**Agent prompt to kick it off:**
> "Implement slice 1 from docs/plans/001-mvp-single-node.md exactly as specified. Tests first. Do not modify internal/log's MemLog semantics. Stop when acceptance is met; summarize deviations."

---

## Slice 2 — durable file log (the heart of the project)

**Goal:** `internal/log` gains `FileLog`: segmented, CRC-framed records, sparse index, recovery-on-open truncating torn tails. `Broker` switches from `MemLog` to `FileLog` behind an interface so tests can choose.

**Design (from ADR-003, restated):**

- Record frame: `[magic u32=0xEV0001? use 0x4556_0001][len u32][crc32c IEEE u32 of (offset+key+value)][offset i64][ts_nanos i64][keyLen u32][valueLen u32][key][value]`. Keep header fixed-size = 4+4+4+8+8+4+4 = 36 bytes.
- Segment files `%016x.log` by base offset; sparse in-memory index: `map[int64]int64` offset→absolute file position, sampled every Nth record (default every 128th) plus all segment base offsets.
- `fsync` on: segment roll, and every 256 appended records or 10ms since last sync (configurable). Expose `Flush()` for tests to force durability points.
- Recovery on `Open`: iterate segments in base-offset order; per record verify magic+len sanity+CRC; on first invalid/incomplete record, `Truncate` file to last good end and stop scanning; rebuild index from scanned records; `HighWatermark = lastOffset + 1`.
- Segment roll at 64 MiB default (`WithSegmentBytes`).
- Delete per ADR-003: whole-segment deletion only.

**Files:** `internal/log/record.go` (frame encode/decode), `internal/log/segment.go` (append/read/recover/truncate), `internal/log/index.go` (sparse index), `internal/log/filelog.go` (`FileLog` implementing new interface `Log` = `Append/Fetch/HighWatermark/Close/Flush`), update `internal/log/log.go` MemLog to satisfy the same interface; `internal/log/filelog_test.go`.

**Tests to write FIRST:**

1. Roundtrip: append 10k random-size records; reopen; read all; byte-identical values; offsets sequential.
2. Segment rollover at a tiny `WithSegmentBytes(4 KiB)`; fetch across segment boundary.
3. **Crash mid-append:** write k full records + a partial frame (half the bytes); reopen; expect exactly k records; no error served; high watermark = k.
4. **Corruption:** flip one byte inside record j's CRC; reopen; records after j are gone (truncate semantics); before j intact.
5. Fetch past end → error; negative offset → error; fetch from segment boundary base.
6. fsync/flush: after `Flush()` + reopen, data present (without Flush, may be lost — assert only Flush path).

**Acceptance:** tests 1–6 green in CI; `cp` a `data/` dir, mount elsewhere, still reads.

**Suggested commit:** `feat(log): segmented file log with CRC recovery and sparse index`

---

## Slice 3 — topic lifecycle & retention

**Goal:** topics persist as a directory layout + metadata snapshot; retention deletes whole segments.

**Files:** `internal/meta/store.go` — `Store` persisting `topics.json` (atomic write: `tmp` + `rename` + `fsync` of dir). `internal/meta/Registry` gains `WithStore(store)`; `Create` writes through; `Load()` at boot rebuilds registry + scans `data/<topic>-*/` to reconcile. `internal/log` gains nothing; retention lives in `internal/broker/janitor.go`: goroutine every minute, per partition, delete whole segments whose `maxOffset < highWatermark - RetentionRecords` or `ModTime` older than `retentionMs`. Config: `WithRetentionBytes/WithRetentionMs` on topic create (HTTP `POST /topics` accepts optional retention).

**Tests FIRST:** store roundtrip (create → reload → topic present); reconcile (create topic, kill, recreate with same name → mismatch error, not silent corruption); retention deletes oldest segment only; fetch from deleted range → `OutOfRange`-style error; create-while-exists → 409 preserved.

**Acceptance:** full restart cycle preserves topics and data; README quickstart updated (data now survives restart — remove the "restart loses data" caveat from slice 1).

**Suggested commit:** `feat(meta): persisted topic registry + segment retention janitor`

---

## Slice 4 — consumer groups (`__consumer_offsets`)

**Goal:** consumer group semantics without an external coordinator: offsets live in an internal compacted topic; two client-side flows — commit and join/claim.

**Design (simplified, documented as such):**

- Topic `__consumer_offsets`, 1 partition (v1). Key = `group\x00topic\x00partition`, value = `{offset i64, generation u32, leader string}`.
- **Compaction:** before append, rewrite the segment dropping superseded keys. Implement `internal/log/compact.go`: `Compact(topic)` — read all, keep last per key, rewrite. Called on offsets-topic open every N commits (default 1000) and on clean shutdown.
- `internal/groups/coordinator.go`:
  - `JoinGroup(group, memberID, topics, sessionTimeoutMs) → (generation, assignment, leader)` — leader = first joiner; rebalance on membership change (join/heartbeat-expiry).
  - `Heartbeat(group, memberID, generation) → error` — `RebalanceInProgress` if generation stale.
  - Assignment: `range` (default) and `roundrobin`; compute from sorted members × sorted partitions.
  - `CommitOffset / FetchOffset(group, topic, partition)`.
- gRPC: extend `SlipwayService` with `JoinGroup`, `Heartbeat`, `LeaveGroup`, `CommitOffset`, `FetchOffset`. HTTP: `GET /groups` (list w/ state), `GET /groups/{g}/offsets`.
- Consumer expiry: membership lease = 10s; reaper goroutine expires silent members every 1s → triggers rebalance.

**Tests FIRST:** two members → range assignment splits 3 partitions 2/1; member crash → after expiry, survivor owns all 3; offset commit/fetch roundtrip across "restart" (close+reopen); generation mismatch on stale heartbeat; compaction preserves last-per-key only.

**Acceptance:** end-to-end mini-scenario in `test/e2e_test.go`: produce 600 msgs (3 partitions), join group with 2 clients, read all, commit offsets, restart broker, rejoin, fetch resumes exactly after committed offsets.

**Suggested commit:** `feat(groups): consumer groups with compacted offsets topic and rebalance`

**Human review gate:** compaction rewrites log files — treat as storage code.

---

## Slice 5 — queue mode (ack / visibility / DLQ)

**Goal:** work-queue semantics on the same topics: competing consumers, at-least-once, dead-letter after N failed deliveries.

**Design:**

- A topic created with `mode: "queue"` (HTTP `POST /queues` = `POST /topics {mode:"queue"}`) behaves as a shared queue: fetch returns unacked records; each delivery tracked as `deliveryID` (uuid).
- Delivery state: in-memory `map[deliveryID]Pending{Topic, Partition, Offset, Attempts, Deadline, Key, Value}` + persisted attempt counts via the queue's own DLQ metadata topic? **Simpler and honest:** attempts tracked in `__consumer_offsets`-adjacent internal topic `__queue_attempts` (key = `queue\x00offset-key`, value = attempt count) compacted like above. Deadline not persisted; on restart all pending deliveries become revisible (at-least-once preserved).
- `Pull(queue, maxMessages, visibilityTimeoutMs)` → batch of deliveries; `Ack(deliveryID)` → mark offset consumed for the internal group `queue-<name>` and clear pending; unacked at deadline → reaper re-enqueues (same offset, attempts+1); attempts > 3 → write to `<queue>.dlq` topic (auto-created).
- API: `Pull`, `Ack` on gRPC; `POST /queues/{q}/pull|ack`.
- Delivery guarantee documented: at-least-once, per-message; ordering best-effort (concurrent consumers).

**Tests FIRST:** producer→2 competing consumers, messages split; kill one mid-processing (simulate via lost ack) → other redelivers after timeout; poison message (always fails ack) → lands on DLQ after 3 attempts; ack unknown ID → error; pending state lost on restart → all in-flight redelivered (at-least-once proof).

**Acceptance:** the full queue scenario passes; `README` quickstart gains a queue example.

**Suggested commit:** `feat(queue): ack/redelivery/DLQ queue mode on shared log`

---

## Slice 6 — hardening, CLI, v0.1.0

**Goal:** operability + release against the ADR-005 checklist.

**Steps:**

1. Metrics: `prometheus/client_golang` — `slipway_produce_total{topic}`, `slipway_fetch_total`, `slipway_fsync_seconds` histogram, `slipway_group_rebalances_total`, queue depth gauge. Expose `/metrics` (promhttp).
2. `cmd/slipway-cli`: subcommands `create-topic|create-queue`, `list`, `produce`, `fetch`, `groups`, `dlq list|redrive` — gRPC client with `--addr` flag; output table + `--json`.
3. Benchmarks (`bench/produce_bench.go`, `fetch_bench.go`): record baseline in `bench/baseline.md`; `make bench` target.
4. Logging: `log/slog` with JSON output; request-scoped logging middleware.
5. CI: add `golangci-lint` job (pinned version), keep vet+test+build.
6. Release checklist: run every ADR-005 acceptance item manually on a clean VM/container; write `CHANGELOG.md`; `git tag v0.1.0`; GitHub release with binaries via goreleaser or raw `go build` matrix.

**Suggested commits:** `feat(obs): metrics + structured logging` · `feat(cli): slipway-cli admin tool` · `chore(release): v0.1.0`

---

## Sequencing summary

| Slice | Depends on | Human review gate | Rough effort |
|---|---|---|---|
| 1 in-memory core | — | gRPC service def | 1 session |
| 2 file log | 1 | **mandatory** (storage) | 1–2 sessions |
| 3 topics/retention | 2 | metadata persistence | 1 session |
| 4 consumer groups | 2 | **mandatory** (compaction) | 1–2 sessions |
| 5 queue mode | 4 | ack semantics | 1 session |
| 6 hardening | 1–5 | release checklist | 1 session |

## Driving this with a coding agent

- Start each session: "Read AGENTS.md, ADR-003, and slice N of this plan. Implement ONLY slice N. Tests first. Human review required for storage diffs — stop and list your changes rather than pushing."
- If the agent proposes deviating from an ADR: it must edit the ADR in the same commit; otherwise reject.
- After each slice: fresh session for review ("Review this diff against ADR-003 invariants. Don't praise; list violations.").

---

## Documentation gate (v0.1.0 close-out, 2026-11-13)

Any change to flags, routes, or RPCs ships with matching updates to [`docs/install.md`](../install.md), [`docs/configuration.md`](../configuration.md), and [`docs/api.md`](../api.md) in the same commit. ADR-008 records this as part of "done"; stale docs are release-blocking.
