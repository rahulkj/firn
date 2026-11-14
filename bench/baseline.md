# Benchmark baseline — v0.1.0 (Apple M3 Pro, darwin/arm64, 2026-11-13)

Command: `go test -bench=. -benchmem -benchtime=2000x ./bench/...`

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---|---|---|---|
| Produce (durable, fsync/append) | 1,969,898 | 350 | 3 | honest number: includes fsync per record |
| Produce (in-memory) | 383 | 472 | 4 | slice-1 MemLog path |
| Fetch (128-record batch) | 133,687 | 101,229 | 478 | durable log, sequential scan from offset |

Observation: fsync-per-append dominates produce latency (~2 ms/op ≈ 500 msg/s
on this machine). ADR-003/ADR-005 call for fsync on flush boundaries
(every N ms / M bytes) rather than per record, but as of 2026-11-13 the ADR-005
produce bar (≥ 50k msg/s) is **retired by amendment**: fsync-per-append is kept,
and a flush-window policy is deferred to a future storage-semantics change
with its own ADR + property tests + mandatory storage review. The 50k msg/s
bar is met in memory by construction; it is not met with fsync-per-append and
will never be claimed as met.

Reproduce: `make bench`.

## Re-run 2026-11-13 (workload matrix, v0.4.x + ADR-014 slice 1 unreviewed; Apple M3 Pro, darwin/arm64)

Command: `go test -bench=Workload -benchmem -benchtime=2s ./bench/ -run=^$` (round-robin seeded fetch, per-partition watermarks)

| Payload | durable produce | windowed produce (2 ms, 1 MiB cap) | fetch (default path) |
|---|---|---|---|
| 256 B | ~368 msg/s (0.09 MB/s) | **~174k msg/s (44 MB/s)** | ~247 MB/s (128-rec windows) |
| 4 KiB | ~347 msg/s (1.4 MB/s) | ~28k msg/s (114 MB/s) | ~1.1 GB/s |
| 64 KiB | ~338 msg/s (22 MB/s) | ~2.4k msg/s (159 MB/s) | ~2.4 GB/s |
| 1 MiB | ~219 msg/s (229 MB/s) | ~782 msg/s (819 MB/s) | ~3.1 GB/s |
| 10 MiB | ~60 msg/s (628 MB/s) | ~88 msg/s (930 MB/s) | ~3.4 GB/s |

Reading it:

- **Durable produce is a constant ~350 msg/s regardless of payload** until the fsync itself hurts — then it's bandwidth-bound (628 MB/s at 10 MiB is about NVMe headroom through fsync). Small-payload enterprise rates do not exist in the default configuration, by design.
- **The window only helps where framing per record is cheap** relative to fsync: massive at 256 B (174k msg/s), large at 64 KiB (159 MB/s), and merely better at 10 MiB (930 MB/s — bandwidth-dominated there too).
- **Fetch scales into multi-GB/s** on large payloads (memory-copy + segment scan bound) — the read path is not the bottleneck for replay-heavy consumers.
- Numbers are provisional until the ADR-014 slice-1 storage review lands.

## Re-run 2026-11-13 (v0.4.0, same hardware)

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| Produce (durable, fsync/append) | 2,846,323 | 350 | 3 |
| Produce (in-memory) | 249 | 472 | 4 |
| Fetch (128-record batch) | 133,210 | 153,481 | 797 |

Same conclusion: fsync-per-append dominates; ~350–500 msg/s per partition range is the honest production number. v0.1.0 baseline kept for history above.

**Why this previously understated reality (2026-11-13): `Broker.Produce` held the broker-wide mutex across the blocking `Append`, serializing every produce behind the flush wait.** Fixed in the ADR-014 implementation: the map lookup is now brief, and `FileLog` serializes its own frames. Same code, same machine, different answer — this file is the proof that methodology matters as much as hardware.

## Re-run 2026-11-13 (ADR-014 slice 1 — pending human storage review; not shipped, `-flush-window` defaults to 0)

Apple M3 Pro, darwin/arm64. Windowed runs use `-benchtime=3s`, `SetParallelism(256)`; the window deliberately stays opt-in. Full suite: `go vet ./... && go test ./... -race -count=1` green.

| Benchmark | Rate |
|---|---|
| Produce, durable, fsync/append (default path) | ~370 msg/s (unchanged) |
| Produce, flush window 2 ms, 1 MiB batch cap, 256 goroutines | **~186,000 msg/s** (~5,364 ns/op) |
| Produce in-memory | ~4.0M msg/s |
| Fetch, 128-record batch | ~1.05M records/s |

Reading those numbers honestly:

- The window path batches *all in-flight staged frames* into one fsync per window. With 256 in-flight producers, ~5,000 msgs land in each 2 ms window.
- With a **single** sequential producer the window cannot batch — one append per window (~1/flush-window ≈ 500–2,000 msg/s, latency-bound by the window + one fsync per window). Batching is a concurrency property, not a setting.
- ack ⇒ durable holds exactly as before (see `internal/log/window_test.go`); the ~186k msg/s are all fsynced before the ack returns. Nothing here is a page-cache artifact.
- These numbers are provisional until a human storage review passes and the flag ships; treat them as evidence for ADR-014, not a release claim.

## Re-run 2026-11-13 (capacity check, v0.4.x, Apple M3 Pro, darwin/arm64)

Command: `go test -bench=. -benchmem -benchtime=2s ./bench/... -run=^$`

| Benchmark | ns/op | B/op | allocs/op | Rate |
|---|---|---|---|---|
| Produce (durable, fsync/append) | 2,697,000 | 356 | 3 | ~370 msg/s |
| Produce (in-memory) | 251 | 646 | 5 | ~4.0M msg/s |
| Produce (durable, 3 partitions, parallel) | 2,676,000 | 376 | 4 | ~374 msg/s aggregate — parallel does **not** scale (device + per-partition fsync) |
| Fetch (128-record batch) | 122,424 | 153,587 | 798 | ~128 rec / 122 µs ≈ **1.05M records/s** |
| Queue pull+ack (in-memory, N=1,000) | n/a (reports deliveries/s) | ~0 | 0 | ~44k deliveries/s (small N — see note) |

Notes:

- **1,000,000 messages is trivial for firn as a *dataset*, impossible as a *rate*.** At the durable produce ceiling (~370 msg/s), writing 1M messages takes ~45 minutes; there is no in-memory cap — capacity is disk (~300 MB per 1M × 256 B records incl. frame overhead ≈ 40 B + payload).
- **Safe single-node envelope (default durability, this machine):** sustained durable produce ≤ ~350 msg/s with stable latency; bursts queue up, they are never dropped (writes serialize at the per-partition FileLog mutex; the device is the ceiling).
- **Client-side default cap:** authenticated clients are limited to `-rate=100` ops/s per API key (flag; `-anon-rate` caps anonymous traffic too). Crossing the broker's safe rate does not cause loss — it causes latency growth.
- **Queue path is fsync-bound twice**: `attempts.Put` per claim + offset commit per contiguous ack, each ~2 ms.
- **Commit path is O(N²) — FIXED 2026-11-13 (plan 006 slice 3, unreviewed).** `internal/groups/offset_store.go`: `Commit` now advances `foldedHWM` directly; re-folds happen only on externally-driven HWM movement. Commit cost is flat (~2.6 ms, fsync-bound) regardless of log length. Profile before the fix had `groups.fold` = 53% of CPU; after the fix the bench shows ~744k deliveries/s (small-N — the scaling win is in the flat-cost unit tests, not the bench). Cross-node regression safety preserved: the coordinator's `CommitOffset` refreshes before applying, so a replicated offset is never regressed by a local commit.
- `BenchmarkFetch` had a latent bug fixed in the same pass: offsets wrapped against the global seed (`i % (seeded-128)`) instead of the per-partition watermark (`seeded/3`), which panics at large `-benchtime`. Fix is test-only; no storage semantics changed.
- ADR-014 slice 4 (opt-in `-internal-flush-window`, 2026-11-13): durable queue pull+ack at 400 messages — internal-window **off: ~192 deliveries/s; internal-window on: ~104 deliveries/s**. Sequential ackers cannot accumulate a window; the two fsyncs per delivery remain the floor. The flag is parked as strictly opt-in (amends the monotone-commits / DLQ-after-3 invariants only under the flag); it is not a recommended default for any measured workload shape. The honest next lever is a batch ack.
- Numbers re-measured on an Apple M3 Pro / NVMe; public-cloud storage will be slower. Treat as an upper bound for dissimilar hardware.
