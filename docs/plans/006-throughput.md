# Plan 006: Throughput (ADR-014 group commit)

Derived from ADR-014. One slice per session, same loop as plans 001–005:

> plan changes land here first → property tests written first → implement → `-race` green → **human storage review (mandatory, non-waivable)** → default stays off ≥ one release → measured numbers only after benchmarks exist.

## Slices

| Slice | Delivers | Review gate |
|---|---|---|
| 1 (**delivered 2026-11-13, awaiting mandatory storage review — do not merge**) | **Group commit / flush window** (`-flush-window`, `-max-batch-bytes`, default off): stage frames in memory; write+fsync the staged batch per window; produce acks only after the window flushes; deterministic rollback of a failed batch (truncate touched segments); `kill -9`-style torn-tail coverage in window_test.go; broker-wide produce mutex no longer held across the blocking append (it serialized every produce behind the window); `bench/baseline.md` re-run with ~186k msg/s windowed produce (256 in-flight producers) vs ~370 msg/s default | **mandatory (storage)** — in flight |
| 2 (**delivered 2026-11-13, awaiting API review**) | **Producer batching**: `ProduceBatch` RPC (one RPC per batch vs per record; hard cap 500; not atomic — records before a failure are durable; retry may duplicate earlier records); `firn-cli produce --batch-file events.jsonl` (JSONL, auto-chunked to 500-record calls); SDK `ProduceBatch`/`KeyValue`/`BatchResult`; docs synced (`api.md`, `developer.md`) | API review — in flight |
| 3 (**delivered 2026-11-13, awaiting mandatory storage-adjacent review**) | **Queue commit coalescing**: `__consumer_offsets` per-commit re-fold eliminated — Commit advances `foldedHWM` directly (local cache == post-append log); `refreshIfMoved` re-folds only on externally-driven HWM movement (replicated apply), preserving read-through; catch-up fold at commit time closes the stale-cache regression window across nodes. Acceptance tests pin all four properties (`internal/groups/offset_store_test.go`); profile had `groups.fold` = 53% CPU | **mandatory (storage-adjacent)** — in flight |
| 4 (**delivered 2026-11-13 — measured with an honest negative result; awaiting review**) | **Internal-topic flush window (opt-in)**: `-internal-flush-window`+`-internal-max-batch-bytes` (default `0`) window-batch **only** `__consumer_offsets` + `__queue_attempts`; `__audit`/`__api_keys`/`__federation*` never window. Startup warning names the two invariant amendments. Measured: sequential consumers are **slower** with it (103 vs 192 deliveries/s — window can't batch sequential ackers), so it's parked as opt-in; the real queue lever is batched ack (see notes) | **mandatory (storage)** — in flight |
| 5 (pending cluster GA plan) | **Cluster-mode throughput**: node-count multiplication math revisited with real per-node numbers; cluster mode out of experimental status (dynamic membership, rebalance/drain) — separate plan (`008-cluster-ga.md`), out of scope here | storage + quorum review |

## Slice details

### Slice 1 — group commit / flush window

- `internal/log.FileLog`: when `flush-window == 0` (default) behavior is byte-for-byte unchanged (existing tests must pass unmodified). When `> 0`: `Append` stages the encoded frame in memory and blocks the caller on the window's flush; the flusher (goroutine, ticker = `-flush-window`, plus size trigger) writes staged frames in order, rolls segments per existing rules, fsyncs, assigns contiguous offsets in staging order, then unblocks all batch producers.
- Failure semantics: fsync failure ⇒ truncate all segments touched by the flushing batch to their pre-flush positions (delete any segment the flush created) ⇒ produce errors for the whole window (no partial acks); rollback failure ⇒ poison fail-closed (appends refused, flushed data readable).
- Memory bounded by `-max-batch-bytes` per partition window.
- HighWatermark() semantics unchanged: unflushed staged frames never exist on disk, so invariant 2 holds by construction (see ADR-014 "Refinements on acceptance").
- Property tests first (ADR-014 list): ack⇒durable (reopen after kill -9), no-ack⇒no-visibility, atomic window visibility, flush-time monotonic allocation, fetch-never-past-watermark during concurrent flush, internal-topic behavior decided by evidence.
- Storage review mandatory. Default stays `-flush-window=0` through at least one release; release notes call the feature out. No headline number in docs until measured.

### Slice 2 — producer batching (delivered, pending API review)

- `ProduceBatch{topic, repeated records}` RPC — one call per batch instead of one call per record; hard cap 500 records per call; `INVALID_ARGUMENT` on empty topic, empty batch, or >500 records.
- Not atomic: records are produced in request order; a failure mid-batch stops the call — records before it are durable, records after are not; retry re-produces the whole batch; retries may duplicate earlier records at new offsets (at-least-once, same as every other path).
- CLI: `firn-cli produce --topic T --batch-file events.jsonl` — JSONL (`{"key": "...", "value": "..."}` per line, `key` optional/absent for round-robin, `-` reads stdin), auto-chunked to 500-record calls. SDK: `firngo.ProduceBatch(ctx, topic, []firngo.KeyValue{...})` returning `[]firngo.BatchResult` (partition + offset per record).
- Exactly one user-visible storage-semantics consideration: offsets within a batch are contiguous per partition.

### Slice 3 — queue commit re-fold (delivered, pending review)

- `internal/groups/offset_store.go`: `Commit` now (a) refreshes the cache when the log moved externally (a replicated apply — closes the stale-cache monotonic-commit window across nodes), then (b) appends and sets `foldedHWM = inner.HighWatermark()` so the next `Get` needs no re-fold. `refreshIfMoved` only re-folds when HWM advanced beyond what we folded — in practice: only after another node's write, which it otherwise couldn't see.
- Acceptance tests (`internal/groups/offset_store_test.go`): no re-fold over 500 local commits; exactly one catch-up fold when a replicated record lands between local commits; exactly one fold when a commit follows an external write; fold equivalence (incremental vs reopened full fold).
- Benchmark: `BenchmarkCommitOffsetScaling` — commit cost now flat (~2.6 ms, fsync-bound) regardless of log length, vs O(N²) before. `bench/` queue pull+ack: ~744k deliveries/s (~17× the pre-fix number, though the small-N bench under-represents the scaling win).
- Storage-adjacent review required; not merged.



### Slice 4 — internal flush windows (opt-in; honest negative result)

- `-internal-flush-window` (default `0`) + `-internal-max-batch-bytes` (default 1 MiB): applies ADR-014 windows to **`__consumer_offsets` and `__queue_attempts` only**. `__audit`, `__api_keys`, `__federation_*` never window (invariant untouched).
- Invariants amended (ADR-003): with the flag, committed offsets may rewind up to the window on hard crash (redelivery; at-least-once preserved); DLQ-after-3 may trigger late (delivery-attempt counts may be lost). Flag-off preserves both invariants verbatim.
- **Measured verdict — do not enable for sequential consumers.** Same machine, same drain loop, 400 durable messages: window-off **192 deliveries/s**, window-on **104 deliveries/s** — sequential ackers can't accumulate a window, so they pay the timer latency with zero batching benefit. The two fsyncs per message (attempts + contiguous-ack commit) remain the floor for sequential ack traffic.
- The honest lever for queue throughput is a **batch ack** (`AckBatch`): one window flush would amortize both fsyncs over N deliveries — candidate for the next slice, pending measurement.
- Clean-close durability covered by `TestInternalFlushWindowCommitSurvivesCleanReopen` (commit → Close → reopen → offset intact).



## Open items

- Cluster-mode placement/scale-out is tracked in plan 008 (not yet written).
- Group-commit interaction with `SealIfOlderThan` (seal requires durable active segment ⇒ flush pending first, then seal — same flush path, no new primitive).
- Metrics for the window (`firn_flush_window_*`) land with slice 1 if cheap; otherwise with slice 5.
