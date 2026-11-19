# ADR-019: Integration driver dependencies (unblocks ADR-016/017/018 follow-ups)

Status: accepted · 2026-11-19 · approved in-session by repo owner (2026-11-19), drivers chosen by the owner with a recommendation.

## Context

ADR-016/017/018 left the driver-dependent connector slices deliberately unbuilt ("dependency-gated"). The owner approved building them on 2026-11-19 and selected drivers. This ADR records those decisions so `go.mod` tells the full story.

## Decision

| Need | Driver | Why |
|---|---|---|
| Kafka source + sink | `github.com/twmb/franz-go` | Pure Go, no cgo, actively maintained, covers produce/fetch; ships a `kfake` in-memory fake cluster used in tests (no broker required). |
| Postgres logical-replication CDC | `github.com/jackc/pglogrepl` (+ `jackc/pgx/pgproto3` stack) | Lowest-level honest path: pgwire replication protocol, no CGo, matches "Postgres logical replication" choice of 2026-11-19. Plugin requirement (wal2json) documented; no SQL parsing on our side. |
| MongoDB change streams | `go.mongodb.org/mongo-driver/v2` | Official driver; change streams + resume tokens are first-class. |
| DynamoDB Streams | `github.com/aws/aws-sdk-go-v2` (+ dynamodbstreams, config, credentials) | Only supported path to DynamoDB Streams; large dependency tree accepted knowingly. |

Rules attached to this approval (house rules, unchanged):

- Integrations stay outside firnd; storage invariants untouched.
- Every connector honors the existing `integration.SourceConnector` contract and the checkpoint runtime; no connector-specific storage paths.
- Secrets only via env vars; configs contain no credentials.
- Each connector ships with unit tests and is honestly labeled "verified against live X" vs "protocol-level tests only" depending on what was actually exercised. Where no live service exists in this environment, that limitation is documented in the connector docs, never papered over.
- Adding any further dependency later requires its own ADR.

## Consequences

- `go.mod` grows by the above modules (franz-go ≈ 15 transitives; pgx stack; mongo-driver; aws-sdk-go-v2's dynamodbstreams tree). This is an explicit, recorded trade-off — not a silent vendoring.
- CI/build times increase accordingly.
- S3-compatible object storage remains covered by the stdlib SigV4 sink from ADR-017 (no aws-sdk for that path).
