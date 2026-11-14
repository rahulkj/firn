# Plan 006: Throughput (ADR-014 group commit)

Derived from ADR-014. One slice per session, same loop as plans 001–005:

> plan changes land here first → property tests written first → implement → `-race` green → **human storage review (mandatory, non-waivable)** → default stays off ≥ one release → measured numbers only after benchmarks exist.

## Slices

| Slice | Delivers | Review gate |
|---|---|---|
| 1 (**delivered 2026-11-13, awaiting mandatory storage review — do not merge**) | **Group commit / flush window** (`-flush-window`, `-max-batch-bytes`, default off): stage frames in memory; write+fsync the staged batch per window; produce acks only after the window flushes; deterministic rollback of a failed batch (truncate touched segments); `kill -9`-style torn-tail coverage in window_test.go; broker-wide produce mutex no longer held across the blocking append (it serialized every produce behind the window); `bench/baseline.md` re-run with ~186k msg/s windowed produce (256 in-flight producers) vs ~370 msg/s default | **mandatory (storage)** — in flight |
| 2 (pending slice 1 review) | **Producer batching**: `ProduceBatch` RPC (one fsync per batch at the SDK/CLI layer), CLI `produce --batch`, SDK `ProduceBatch`, docs/examples | API review |
| 3 (**delivered 2026-11-13, awaiting mandatory storage-adjacent review**) | **Queue commit coalescing**: `__consumer_offsets` per-commit re-fold eliminated — Commit advances `foldedHWM` directly (local cache == post-append log); `refreshIfMoved` re-folds only on externally-driven HWM movement (replicated apply), preserving read-through; catch-up fold at commit time closes the stale-cache regression window across nodes. Acceptance tests pin all four properties (`internal/groups/offset_store_test.go`); profile had `groups.fold` = 53% CPU | **mandatory (storage-adjacent)** — in flight |
| 4 (pending slice 3 + slice 1 evidence) | **Flush window on internal topics** if evidence says fsync-on-commit dominates (`__consumer_offsets`, `__queue_attempts`, `__audit`, `__api_keys`) — may reuse slice 1 mechanism with a tighter window or remain fsync-per-append; decided by measurement | **mandatory (storage)** |
| 5 (pending cluster GA plan) | **Cluster-mode throughput**: node-count multiplication math revisited with real per-node numbers; cluster mode out of experimental status (dynamic membership, rebalance/drain) — separate plan (`008-cluster-ga.md`), out of scope here | storage + quorum review |

## Slice details

### Slice 1 — group commit / flush window

- `internal/log.FileLog`: when `flush-window == 0` (default) behavior is byte-for-byte unchanged (existing tests must pass unmodified). When `> 0`: `Append` stages the encoded frame in memory and blocks the caller on the window's flush; the flusher (goroutine, ticker = `-flush-window`, plus size trigger) writes staged frames in order, rolls segments per existing rules, fsyncs, assigns contiguous offsets in staging order, then unblocks all batch producers.
- Failure semantics: fsync failure ⇒ truncate all segments touched by the flushing batch to their pre-flush positions (delete any segment the flush created) ⇒ produce errors for the whole window (no partial acks); rollback failure ⇒ poison fail-closed (appends refused, flushed data readable).
- Memory bounded by `-max-batch-bytes` per partition window.
- HighWatermark() semantics unchanged: unflushed staged frames never exist on disk, so invariant 2 holds by construction (see ADR-014 "Refinements on acceptance").
- Property tests first (ADR-014 list): ack⇒durable (reopen after kill -9), no-ack⇒no-visibility, atomic window visibility, flush-time monotonic allocation, fetch-never-past-watermark during concurrent flush, internal-topic behavior decided by evidence.
- Storage review mandatory. Default stays `-flush-window=0` through at least one release; release notes call the feature out. No headline number in docs until measured.

### Slice 2 — producer batching

- `ProduceBatch{topic, repeated records}` RPC; broker funnels into per-partition windows; one fsync per batch end-to-end; CLI `firn-cli produce --batch-file file.jsonl`; SDK `ProduceBatch`.
- Exactly one user-visible storage-semantics consideration: offsets within a batch are contiguous per partition.

### Slice 3 — queue commit re-fold (delivered, pending review)

- `internal/groups/offset_store.go`: `Commit` now (a) refreshes the cache when the log moved externally (a replicated apply — closes the stale-cache monotonic-commit window across nodes), then (b) appends and sets `foldedHWM = inner.HighWatermark()` so the next `Get` needs no re-fold. `refreshIfMoved` only re-folds when HWM advanced beyond what we folded — in practice: only after another node's write, which it otherwise couldn't see.
- Acceptance tests (`internal/groups/offset_store_test.go`): no re-fold over 500 local commits; exactly one catch-up fold when a replicated record lands between local commits; exactly one fold when a commit follows an external write; fold equivalence (incremental vs reopened full fold).
- Benchmark: `BenchmarkCommitOffsetScaling` — commit cost now flat (~2.6 ms, fsync-bound) regardless of log length, vs O(N²) before. `bench/` queue pull+ack: ~744k deliveries/s (~17× the pre-fix number, though the small-N bench under-represents the scaling win).
- Storage-adjacent review required; not merged.



### Slice 4 — internal-topic flush windows

- Decided by measurement after slice 3, likely reusing slice 1's machinery with a much tighter default window or fsync-per-append retained on audit/keys for paranoid writes. Do not slip into this slice silently.

## Open items

- Cluster-mode placement/scale-out is tracked in plan 008 (not yet written).
- Group-commit interaction with `SealIfOlderThan` (seal requires durable active segment ⇒ flush pending first, then seal — same flush path, no new primitive).
- Metrics for the window (`firn_flush_window_*`) land with slice 1 if cheap; otherwise with slice 5.
