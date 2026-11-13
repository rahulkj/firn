# Plan 001: MVP single-node broker (slices 1–6)

Work proceeds **one slice per session**, always: implement → read the diff → verify (`go vet ./... && go test ./...`) → commit. Never start a slice with uncommitted changes or a red test.

## Slice 1 — in-memory core (START HERE)

- `internal/meta`: topic registry (done, v0 skeleton)
- `internal/log`: `MemLog` append/fetch (done, v0 skeleton)
- `internal/broker`: produce/fetch orchestration over MemLog, partition assignment = FNV(key) % partitions
- `internal/api`: gRPC service `EventingService { Produce, Fetch }` + HTTP admin: `POST /topics`, `GET /topics`
- Tests: end-to-end produce→fetch over in-process gRPC (bufconn); key-based partition stickiness; unknown-topic error
- **Acceptance:** `eventingd` produces and fetches 10k messages in-memory; restart loses data (documented)

## Slice 2 — durable file log

- `internal/log`: segment writer, sparse offset index, recovery-on-open (CRC validate, truncate torn tail)
- Fault-injection test: kill mid-append; reopen; assert clean state and no partial record visible
- **Acceptance:** produces survive `kill -9`; recovery test in CI

## Slice 3 — topics & retention

- Topic creation persists to a metadata file; segments named by base offset
- Retention: delete whole segments older than TTL / over byte budget

## Slice 4 — consumer groups

- `__consumer_offsets` compacted internal topic; commit/fetch offset APIs
- Group coordinator-lite: join/sync, range + roundrobin assignment, session expiry via heartbeat

## Slice 5 — queue mode

- `internal/queue`: ack, visibility timeout (default 30s), redelivery, DLQ after 3 attempts
- Acceptance test: producer → 2 competing consumers → crash one consumer → other redelivers and completes

## Slice 6 — hardening & v0.1.0

- Metrics (Prometheus: produce/fetch rate, fsync latency, under-replicated = 0/1)
- `eventing-cli`: create-topic, describe-topic, produce, fetch, group-offsets
- Tag v0.1.0; release notes against ADR-005 acceptance list

## Session rules

1. Paste only the invariants from ADR-003/AGENTS.md relevant to the slice — never the whole history.
2. Storage diffs require human review, no exceptions.
3. If the slice needs a plan change: edit this file first, then implement.
