# ADR-014: Group commit / flush window — batching appends into one fsync

Status: **accepted** · proposed 2026-11-13, accepted 2026-11-13 (human, in-session) · implementation not merged — human storage review of the implementation diff is still mandatory before it ships (acceptance criteria §5).

## Context

Firn's produce path is fsync-per-append (`WithSyncOnAppend(true)`): one device flush per acknowledged produce. Measured on Apple M3 Pro (2026-11-13, `bench/baseline.md`): ~2.0 ms/op ≈ **350–500 msg/s** per node, durable. Parallel writers across partitions do not scale (374 msg/s vs 370). The 50k msg/s produce bar was retired by ADR-005 amendment (2026-11-13) rather than faked; 1M msg/s is three orders of magnitude beyond the shipped design.

That is a fine default for control-plane workloads and a disqualifier for enterprise event volume. This ADR proposes the one lever that changes the arithmetic: **bounded flush windows**, aka group commit.

What this ADR explicitly touches: **ADR-003, invariant 2** — see "Watermarks (as accepted)". As accepted, the mechanism keeps invariant 2 verbatim; no amendment of its wording is required. No other invariant changes.

## Decision (accepted 2026-11-13; mechanism refined during pre-implementation analysis — see "Refinements on acceptance")

Buffer appends per partition into a window; **write the staged frames to the segment at flush time**; make the produce call return only after its batch flushes.

- New flags: `-flush-window` (duration, default `0` = fsync-per-append, byte-for-byte v0.4.0 behavior) and `-max-batch-bytes` (integer, default 1 MiB). `-flush-window=0` disables batching entirely; nothing else changes.
- **Offsets are allocated at flush time, not at append time** — frames stage in memory until the window flushes, and the flush assigns contiguous offsets in staging order. Invariant 1 holds exactly (monotonic from 0, never reused, never gapped by a failed batch).
- A produce acks **only after the flush that contains its frame completes**. No ack is ever issued for a record whose fsync has not returned successfully.
- Window rule per partition: flush when `pendingBytes >= -max-batch-bytes` **or** when the oldest pending frame has waited `-flush-window`. Empty windows cost nothing — no fsync is issued when there is nothing pending.
- Per-partition windows only. No cross-partition batches (ordering and invariants stay per partition).
- Compression is a client concern (lz4/zstd client-side); this ADR does not add broker-side compression.

## Refinements on acceptance (2026-11-13, pre-implementation)

Two changes from the proposed text, both discovered while reading `internal/log/filelog.go` before writing code. They are listed here because an ADR that doesn't track its own evolution rots.

1. **Stage frames in memory; write to the file at flush.** The proposed text implied appends hit the file immediately with an "append watermark" and a "flushed watermark" (a split of ADR-003 invariant 2). That split is unnecessary and unsafe: after a plain `kill -9` (not power loss), frames written but not yet fsync'd can persist in the page cache and would then be accepted at recovery as valid records — visible without an ack, breaking the watermark clamp, and offsets would have had to be burned for aborted batches (violating monotonicity-without-gaps, invariant 1). Holding staged frames in memory means the segment contains only flushed frames, always.
2. **Therefore: no watermark split, invariant 2 unchanged.** "Fetch never returns records at offset ≥ high watermark" survives verbatim — the high watermark simply never counts unflushed work, because unflushed work was never written. No ADR-003 amendment is required. `HighWatermark()`, retention, replication, sealing, and DLQ all keep their existing meaning without modification.

Consequences carried forward: ADR-003 needs no amendment (invariant 2 in its original form holds by construction); the "ack ⇒ durable" property tests are unchanged; crash-recovery code is also unchanged (it already truncates anything past the first bad frame; staged-but-never-written frames simply never appear).

## Watermarks (as accepted)

Single high watermark, unchanged. Because staged frames are never written to the segment before their flush, the log on disk (and `HighWatermark()`) reflect durable records only. No split, no fetch-side clamp change: `Fetch` keeps its existing `offset >= l.hwm` refusal, and no record can be returned before it is fsynced.

## Failure modes and recovery

- **Process crash mid-window**: batch is lost; its produces were never acked; producers retry (at-least-once, invariant 5 unchanged). No torn record is ever visible (invariant 3): unflushed frames were never written to the segment at all, so there is nothing to scan.
- **fsync failure**: flush begins by snapshotting the touched segments' write positions, writes the staged frames in order, then fsyncs. **On fsync failure, the flush deterministically truncates every segment it touched back to its pre-flush position** (deleting any segment created by this flush) — no unacked frame can survive on disk. Every produce waiting on the failed window returns an error (no partial acks, ever); `HighWatermark()` does not advance; offsets of failed frames are burned with the batch (clients retry with new offsets — at-least-once, invariant 4/5 unchanged). If the rollback itself fails, the log is poisoned fail-closed (appends refused, flushed data still readable) until restart — never silently half-applied. Callers must treat Produce errors as unknown outcome and retry — same contract as today's fsync failure path.
- **Crash between flush return and ack delivery** (TCP dies after flush, before the client sees the response): the record **is durable** but the client doesn't know it produced — the existing at-least-once producer-retry story covers this; unchanged by this ADR.
- **Memory pressure**: the window bounds staged bytes to `-max-batch-bytes` per partition per window — bounded by config, not by load. (Bytes are held pre-encode; keys/values are not copied twice beyond the batch.)

## Ordering

Offsets remain monotonic from 0 per partition, never reused (invariant 1). Flush order equals append order per partition. A consumer can never observe record N without also being able to observe N−1 (same flush granularity). The current "visibility at single-append granularity" becomes "visibility at window granularity" — a strict coarsening, never a reordering.

## Property tests required before any merge (write these first)

1. **ack ⇒ durable**: for a batch of size k, kill -9 (or fsync-inject failure) immediately after `Produce` returns nil; on reopen, all k records are present and CRC-valid.
2. **no ack ⇒ no visibility**: crash during a window; on reopen, no record that wasn't acked is visible to Fetch/replica/peer.
3. **atomic window**: a window either appears completely or not at all; a fetch racing the flush returns either everything before the window or everything up through it — never half of a window, never records past the flush point.
4. **monotonic allocation under windowing**: offsets are allocated at flush time in staging order — strictly increasing, never reused, no gaps in the durable log (a failed window burns its offsets; retry takes new ones). Invariant 1 holds without any carve-out for client retries.
5. **fetch watermark**: Fetch and the replication push path never return/serve records past the durable high watermark, including during a concurrent flush — including the frames written moments earlier whose flush has not yet completed.
6. **internal topics** (`__consumer_offsets`, `__queue_attempts`, `__audit`, `__api_keys`): same windowing applies or they remain fsync-per-append via a separate, tighter default — decided by evidence; internal correctness must not ride on looser windows silently.

Fault injection to extend the existing kill -9 harness (`internal/log`) to the window path. Property tests live in-tree (`internal/log/*_test.go`), not in bench/.

## Performance model — claims are measurements, not marketing

Flush rate is unchanged (~500 fsyncs/s on the reference machine). Throughput scales with records-per-window:

| msgs per 2 ms window | theoretical msg/s | honest expectation |
|---|---|---|
| 10 | 5k | plausible near-term |
| 100 | 50k | requires real batching in the hot path (client + broker) |
| 1,000 | 500k | bounded by framing/serialization CPU before fsync; Aspirational |

**No throughput number is to be claimed in docs, README, release notes, or the comparison matrix until it is measured with `bench/` on this machine and a Linux/NVMe reference host.** The ADR-005 pattern (amend the bar with evidence) applies.

## Configuration and defaults

- `-flush-window` default `0`: byte-for-byte v0.4.0 semantics for every existing deployment. Opt-in only.
- Suggested opt-in starting point once accepted: `-flush-window=2ms -max-batch-bytes=1048576`.
- Metrics: `firn_flush_window_records` (histogram), `firn_flush_window_bytes`, `firn_flush_window_wait_seconds`, `firn_flush_failures_total` (for alerting on fsync failure).

## Risks

- **Operational surprise**: a cluster running with `-flush-window=2ms` loses up to 2 ms of acknowledged-looking produce on hard crash (it never acked those, but operators may misread this as "5 nines durability"). Docs and `/healthz` version output must state the window plainly; `/metrics` exposes it; `GET /topics` response could carry it — decided in the slice plan.
- **Tail latency**: first message in an underloaded window waits `-flush-window`. Bounded, documented, tunable; low-load latency-sensitive apps keep default `0`.
- **Empty-window fsync storm**: explicitly impossible — no flush is issued with empty pending set.
- **Interaction with queue path**: the queue redelivery loop already had an fsync-per-message shape; windowing helps it only if the O(N²) `__consumer_offsets` re-fold is fixed (separate slice, `docs/plans/006-throughput.md` after this ADR lands).
- **Replication**: block replication serves only flushed blocks; lease-holder liveness unchanged. ReplicaPut semantics unchanged.

## Alternatives considered and rejected

- **Async fsync without a flushed watermark** (ack after append, sync eventually): violates invariant 2/3 outright — records could be "acknowledged" and then vanish. Categorically rejected.
- **Always-on batching, small default**: changes the product's #1 documented identity (fsync-per-append) without opt-in; rejected until evidence exists.
- **Rely on filesystem/write-back cache** (`sync_file_range`, `fdatasync` tuning without watermarking): same correctness class as async fsync; rejected.
- **Horizontal scale without windowing**: ~270 nodes for 100k msg/s — operationally absurd; out of scope.
- **Group commit across internal topics only**: helps the O(N²) fix marginally; doesn't touch user produce throughput; folded into this design instead.

## Acceptance criteria and review gates

1. Human accepts this ADR (storage semantics). No code before this sentence.
2. Property tests above are written and failing in a branch; then the implementation makes them pass.
3. `kill -9` storm test against a live 3-node cluster with mixed flush windows.
4. Human storage review of the diff (mandatory; non-waivable).
5. Benchmarks recorded in `bench/baseline.md` alongside the existing ones; `docs/comparison.md` and README updated only with measured numbers.
6. Release notes call the feature out by name; default remains off through at least one release.

## Follow-ups (not this ADR)

- `docs/plans/006-throughput.md` after acceptance — sequencing: this ADR → queue `__consumer_offsets` re-fold fix → producer batching API (`ProduceBatch`) + CLI → cluster mode GA work (dynamic membership, rebalancing) — each its own slice, plan changes land in the plan file first.
- ADR-003 gets an amendment note pointing here once accepted.
- `docs/comparison.md` gets an update with measured post-windowing numbers.
