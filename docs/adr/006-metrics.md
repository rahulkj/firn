# ADR-006: Metrics — prometheus/client_golang, exposed at /metrics

Status: accepted · 2026-11-13 · delivered (v0.1.0)

## Context

Slice 6 (docs/plans/001-mvp-single-node.md) requires operability metrics — produce/fetch rates, fsync latency, group rebalances, queue depth — exposed for Prometheus scraping. ADR-005's release checklist treats MVP operability as a requirement; metrics were never given an ADR.

## Decision

- Add `github.com/prometheus/client_golang` (with `client_golang/prometheus/promhttp`) as a dependency.
- Metrics are exposed on the existing HTTP admin listener at `/metrics` (promhttp handler) — no separate port.
- Metric set for v0.1.0: `eventing_produce_total{topic}` (counter), `eventing_fetch_total{topic}` (counter), `eventing_fsync_seconds` (histogram), `eventing_group_rebalances_total` (counter), `eventing_queue_depth` (gauge: pending + re-queued messages across queues).
- `internal/log` gains a package-level `FsyncObserver func(time.Duration)` hook instead of importing prometheus — the storage package stays dependency-free; the metrics package registers the observer at process start. Same rationale for the groups coordinator: a plain `Rebalances()` accessor, not a prometheus import.

## Consequences

- One new third-party dependency tree (client_golang + its transitive deps). Accepted by the slice-6 plan, which names client_golang explicitly.
- `/metrics` is unauthenticated like the rest of the admin surface (v1: single-node, no auth — ADR-001 non-goal). Scrape restrictions are a deployment concern.
- Cardinality stays bounded by topic count (operator-created); no per-message labels.
