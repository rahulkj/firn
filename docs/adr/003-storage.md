# ADR-003: Storage engine — append-only segment log, and its invariants

Status: accepted · 2026-11-13 · **Read before touching internal/log or offsets**

## Context

Both streaming and queue semantics need one thing: an ordered, durable, replayable sequence per partition. This is the single hardest component in the system and the place where subtle bugs destroy data.

## Decision

- One **append-only segment log per partition**: `data/<topic>-<partition>/000000.log` etc.
- Records: length-prefixed, CRC32-checksummed frames: `[len u32][crc u32][offset i64][ts i64][key][value]`.
- Sparse in-memory index offset→file position, rebuilt on open by scanning (v1); persisted index later.
- **Crash recovery on open:** scan to end; validate CRC per record; truncate the torn tail; rebuild index. This is mandatory and testable — `fault-injection tests` must crash mid-write and assert the log opens clean.
- **fsync policy:** fsync on segment roll + on `flush()` boundaries requested by the broker (e.g., every N ms or M bytes). Never silently skip.
- Retention by bytes/time deletes whole segments only.

## Invariants (enforced by tests, reviewed by humans)

1. Offsets are assigned monotonically from 0 per partition; never reused.
2. Fetch never returns records at offset ≥ high watermark.
3. After recovery, no torn/partial record is visible.
4. High watermark = next offset to assign; durable across restarts.
5. Segment files are append-only between checkpoint boundaries; deletes are whole-segment.

## Consequences

- v1 is single-node and fsync-bounded; replication (ADR-006, later) will wrap this log, not replace it.
- Property tests are mandatory here: append/fetch/recover round-trips (see internal/log/log_test.go as the seed).
