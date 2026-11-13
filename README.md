# eventing

A from-scratch eventing broker in Go providing **streaming** (Kafka-like: topics, partitions, offsets, consumer groups) and **queue** (RabbitMQ-like: ack, redelivery, DLQ) semantics over a shared append-only log.

Status: **scaffold / pre-MVP** — see [`docs/plans/001-mvp-single-node.md`](docs/plans/001-mvp-single-node.md) and [`docs/adr/`](docs/adr/).

## Quick start

```sh
make build   # go build ./...
make verify  # go vet ./... && go test ./...
go run ./cmd/eventingd   # serves /healthz on :9092
```

## Design docs (read in order)

1. [ADR-001 Scope](docs/adr/001-scope.md)
2. [ADR-002 API surface](docs/adr/002-api.md)
3. [ADR-003 Storage engine & invariants](docs/adr/003-storage.md)
4. [ADR-004 Consistency stance](docs/adr/004-consistency.md)
5. [ADR-005 MVP definition](docs/adr/005-mvp.md)

Working with coding agents on this repo: read [`AGENTS.md`](AGENTS.md) — storage invariants are mandatory context, and storage diffs always need human review.
