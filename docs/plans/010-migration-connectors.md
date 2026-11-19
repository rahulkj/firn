# Plan 010: Migration connectors (ADR-020)

Derived from ADR-020 (accepted 2026-11-19). One slice per session, same loop as plans 001–009.

> plan changes land here first → tests first → implement → `go vet ./... && go test ./... -race` green → human review → commit.
> Migration-grade promises: at-least-once everywhere; no storage-path changes; no per-dialect storage semantics; SQL writes always in explicit transactions.

## Context

Customers asked for connectors to "any commonly used data stores" for migration off legacy systems. ADR-020 draws the line: one generic SQL family (any `database/sql` driver) + first-class messaging (RabbitMQ, NATS, Redis Streams, IBM MQ) + HTTP-family sinks (Elasticsearch, ClickHouse), with deep per-dialect CDC deferred.

## Slices

| Slice | Delivers | Live verification |
|---|---|---|
| 1 (**delivered 2026-11-19**) | **Generic SQL source** (`internal/integration.SqlPollingConnector`, `firn-ingest -source-sql`): polls any table with a watermark column (`int64` or RFC 3339), key columns → record key, resume via watermark, empty-safe, injection-safe identifier validation. Covers Postgres, MySQL/MariaDB, SQL Server, Oracle, SQLite, Cockroach/Yugabyte (pgx wire). | SQLite (in-memory) round trip (id/timestamp watermarks, resume at midpoint, drain semantics). Oracle Free 23ai (FREEPDB1) upsert round trip via MERGE (FROM dual, parenthesized ON — 23ai+ and ≤19c compatible). MariaDB 11 upsert via ON DUPLICATE KEY UPDATE. |
| 2 (**delivered 2026-11-19**) | **Generic SQL sink** (`firn-egress type: sql`): per-batch explicit transaction; dialect-aware conflict clauses (pgx/sqlite `ON CONFLICT`, mysql `ON DUPLICATE KEY UPDATE`, sqlserver/oracle `MERGE` with `FROM dual` + parenthesized `ON`); type coercion (integral floats → int64); optional `append_only`; optional `columns` allow-list; `key_columns` required for upsert. | SQLite upsert/append/type-mapping round trips. MariaDB upsert. Oracle upsert. SQL Server blocked locally (arm64 host can't run SQL Server under emulation) — same code path as Oracle's MERGE; stub-tested only. |
| 3 (**delivered 2026-11-19**) | **RabbitMQ source & sink** (`firn-ingest -source-amqp` / `firn-egress type: rabbitmq`): AMQP 0.9.1, durable queue, persistent messages, manual ack (source) and broker publish-confirms (sink), default exchange routing (queue-name routing key). | rabbitmq:3 (port 5672) — produce → sink → consume verify; connector fetch → produce loop. |
| 4 (**delivered 2026-11-19**) | **NATS source & sink** (`-source-nats` / `type: nats`): JetStream durable pull consumer (recommended) with AckExplicit; core NATS publish (delivery attempt; at-most-once when no stream); stream name optional. | nats:2 `-js` — JetStream publish/fetch round trips on both paths. |
| 5 (**delivered 2026-11-19**) | **Redis Streams source & sink** (`-source-redis-streams` / `type: redis-streams`): XREADGROUP + XACK (source), XADD MAXLEN ~ 10000 (sink); one stream per route; consumer group default `firn-<name>`. | redis:7 (port 6379) — XADD/XREAD round trips. |
| 6 (**delivered 2026-11-19**) | **IBM MQ source & sink via mqweb REST API** (`-source-ibmmq` / `type: ibmmq`): browse (GET) → produce → consume (DELETE); publish (POST) with persistent mode and correlationId. No IBM client libs; single-consumer semantics. | **stub-tested only** — mqweb REST contract tests; the IBM MQ image does not run on this arm64 host. |
| 7 (**delivered 2026-11-19**) | **Elasticsearch & ClickHouse sinks** (`type: elasticsearch` / `type: clickhouse`): bulk `_bulk` (index/_id/pipeline) and `INSERT ... FORMAT JSONEachRow` via HTTP; native no-dependency drivers; clickhouse auth via `X-ClickHouse-User/Key`. | elasticsearch:7.17 (port 9200) — bulk index + count verify. clickhouse:latest (port 8123) — JSONEachRow insert + count verify. |
| 8 (**delivered 2026-11-19**) | Docs (migration playbooks in `docs/migration-playbooks.md`), ADR-020 implementation-notes addendum, plan update, ADR-013 flip, CHANGELOG | this file |

## Slices NOT delivered (named, per ADR-020 §4)

- MySQL binlog / SQL Server CDC / Oracle LogMiner (native CDC per dialect).
- IBM MQ native client (mq-golang + MQ C client), IBM MQ live verification.
- Snowflake, BigQuery, Redshift native sinks.
- DB2 (cgo driver), HBase, Cassandra, Couchbase, Neo4j, Teradata, Vertica, Greenplum, Netezza (no demand yet; each would need an own driver ADR).
- Kafka Connect wire compatibility.
- SQL Server local verification (hardware block).

## Delivery notes (2026-11-19)

- SQL placeholder styles are per-dialect: pgx `$N`, sqlite/mysql `?`, mssql `@pN`, oracle `:N`; subqueries for Oracle get `FROM dual` (works on 19c+ and 23ai); Oracle MERGE requires parenthesized ON (enforced by builder).
- NATS Fetch uses the legacy JetStream API with `nats.ContextOpt` deadline context (FetchMaxWait lives in the new jetstream package; not used).
- AMQP publish routes via the default exchange with routing key = queue name (record key in `correlationId` + `key` header), which is the only shape that reliably lands messages in queues without pre-created exchange bindings.
- Redis `XREADGROUP` created with start id `0` (stream beginning) so fresh deployments see pre-existing entries; consumers that roll the group forward must handle redelivery.
- All four messaging systems live-verified on this host (Port 5672/4222/6379/8123/9200).
