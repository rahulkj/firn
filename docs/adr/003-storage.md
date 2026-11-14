# ADR-003: Storage engine — append-only segment log, and its invariants

Status: accepted · 2026-11-13 · delivered (v0.1.0) · **read before touching internal/log or offsets**

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

(2026-11-13 note, ADR-014): no amendment of these invariants was required for group commit / flush windows on partition logs. Staged frames never reach the segment before flush, so the fetch-watermark invariant holds verbatim and the high-watermark invariant is flush-aligned at durability time; offsets are allocated at flush time in staging order, preserving monotonic-assignment exactly.

(2026-11-13 note, ADR-014 slice 4, opt-in `-internal-flush-window`): the flush window MAY be applied to `__consumer_offsets` and `__queue_attempts` as an explicit operator choice. Consequences, valid only outside the flag:
- **monotone committed offsets**: committed offsets may rewind up to `-internal-flush-window` on hard crash (offsets in the lost window are redelivered — at-least-once, but not strict monotonicity until the next commit);
- **DLQ after N attempts**: delivery-attempt counts may be lost in the window, so the DLQ diversion may trigger later than N attempts for a message whose counts it lost; the at-least-once redelivery guarantee itself is unchanged;
- `__audit`, `__api_keys`, `__federation_*` are never windowed regardless of the flag.
Default (0) preserves all invariants exactly as written.

## Consequences

- v1 is single-node and fsync-bounded; replication (ADR-006, later) will wrap this log, not replace it.
- Property tests are mandatory here: append/fetch/recover round-trips (see internal/log/log_test.go as the seed).
