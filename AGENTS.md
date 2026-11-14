# AGENTS.md — firn

Firn is a from-scratch event broker in Go providing **streaming** (topics/partitions, replay, consumer groups) and **queue** (ack/redelivery) semantics over a shared append-only log. See `docs/adr/` for the decisions that matter (010 = federation).

## Commands

- Build: `go build ./...`
- Test: `go test ./...` (single test: `go test ./internal/log -run TestName -v`)
- Vet: `go vet ./...`
- Lint: `golangci-lint run`
- Bench: `go test -bench=. -benchmem ./bench/...`
- **Verify before done: `go vet ./... && go test ./...`**

## Layout

- `cmd/firnd` — broker daemon (serves `/healthz` for now)
- `internal/log` — append-only log (storage invariants live here; sacred)
- `internal/meta` — topic/partition metadata registry
- `internal/broker` — produce/fetch orchestration, partition assignment
- `internal/queue` — ack/redelivery/DLQ on the same log
- `internal/api` — gRPC/HTTP surface (clean API, **not** wire-compatible with any other broker)
- `docs/adr` — architecture decision records (read 003 before touching storage; read 012 for the component/topology map)
- `docs/plans` — implementation plans; work proceeds slice by slice

## Conventions

- Go 1.27+, stdlib-first; third-party deps only with an ADR
- `context.Context` as first argument for I/O; wrap errors with `%w`
- Interfaces at the consumption site, small and unexported
- Table-driven tests; property tests required for any storage semantics change
- Offsets/watermarks are `int64`; never reuse them for anything else

## Storage invariants (docs/adr/003-storage.md — do not change casually)

1. Offsets assigned monotonically from 0, per partition, never reused
2. Fetch never returns records at offset ≥ high watermark
3. After crash/recovery, no torn tail record is visible to consumers
4. Committed group offsets advance monotonically unless explicitly reset
5. Unacked queue messages are redelivered (at-least-once); DLQ after N attempts

(Note, ADR-014 slice 4): `-internal-flush-window > 0` is an explicit operator opt-in that may rewind commits by up to the window on hard crash and may delay DLQ diversion (attempts lost in the window). At-least-once redelivery still holds. Default off preserves 4 and 5 exactly; `__audit`/`__api_keys`/`__federation*` never window.

## Rules for agents working in this repo

- **Storage code (`internal/log`, `internal/meta`, anything touching disk or offsets):** generate, then STOP and request human review. Never merge storage diffs unreviewed, even if tests pass.
- Write the property/acceptance test **first** for any change to produce/fetch/ack/redeliver semantics.
- Do not add dependencies without an ADR and human approval.
- Keep PRs/diffs to one slice from `docs/plans/`; if a slice needs to change the plan, change the plan file first.
- Non-goals (v1): replication beyond a single cluster, exactly-once, wire compatibility with other brokers, auth/multi-tenancy hardening. Do not build these even if they seem "easy to add".
