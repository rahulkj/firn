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
