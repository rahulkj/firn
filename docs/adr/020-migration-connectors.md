# ADR-020: RDBMS / messaging / data-store connector expansion — migration toolkit scope

Status: accepted · 2026-11-19 · approved in-session by repo owner.

## Context

ADR-016/017/019 delivered ingress/egress/pipeline reference implementations with Kafka, Postgres CDC, MongoDB, and DynamoDB. Customers asked for the same treatment for the rest of the estate, explicitly framed around **migration off legacy systems**: "allow connectors to any RDBMS database (in and out), RMQ, IBM MQ, etc.", and "solve for any commonly used data stores that customers are trying to migrate off."

The honest constraint: there are hundreds of products. This ADR fixes the strategy so the explosion is bounded, honest, and uniform instead of a laundry list of one-off drivers.

## Decision

### 1. One generic SQL family, not N dialect connectors

Any RDBMS with a `database/sql` driver is covered by **one** source and **one** sink:

- **Source (`SqlPollingConnector`, `firn-ingest -source-sql`)**: poll a table (or arbitrary query) by a **watermark column** (`updated_at`/auto-increment pk/etc.), ordered ascending, in batches. Position = watermark value. Works identically on Postgres, MySQL/MariaDB, SQL Server, Oracle, SQLite, CockroachDB/Yugabyte (wire-compatible), and anything else with a driver. No per-dialect plugins, no WAL/binlog dependency. This is the baseline for *every* RDBMS; native CDC slots remain dialect-specific extras (Postgres already shipped; others later if demanded).
  - Watermark types: `int64` (sequences) or RFC 3339 timestamp. Rows are read `WHERE watermark > :last ORDER BY watermark ASC LIMIT :n`. Checkpointing is post-produce, at-least-once, duplicates expected and deduplicated downstream by key.
- **Sink (`SqlSink`, `firn-egress type: sql`)**: batch upsert into a table in one transaction. Dialect-aware conflict clauses: Postgres/Cockroach/Yugabyte/SQLite `INSERT ... ON CONFLICT`; MySQL/MariaDB `ON DUPLICATE KEY UPDATE`; SQL Server/Oracle `MERGE`; plain `INSERT` (append-only) anywhere. Key columns configurable. Columns: all-or-explicit list; values passthrough-typed (driver coerces). This is sufficient for "state mirror in Postgres from Firn" and for table-to-table migration bridges.

Dialects covered in v1 of this family: **Postgres** (`lib/pq`-class via pgx, already in tree), **MySQL/MariaDB** (`go-sql-driver/mysql`), **SQL Server** (`microsoft/go-mssqldb`), **Oracle** (`sijms/go-ora/v2` — pure Go, no client install), **SQLite** (`modernc.org/sqlite`, pure Go). Other dialects require only a registered driver name in config; no new code path.

### 2. Messaging: first-class connectors, live-tested

| System | Source | Sink | Client | Tested |
|---|---|---|---|---|
| RabbitMQ | `firn-ingest -source-amqp` | `firn-egress type: rabbitmq` | `rabbitmq/amqp091-go` | dockerized `rabbitmq:3` |
| NATS | `firn-ingest -source-nats` | `firn-egress type: nats` | `nats-io/nats.go` | dockerized `nats:2` |
| Redis Streams | `firn-ingest -source-redis-streams` | `firn-egress type: redis-streams` | `redis/go-redis/v9` | dockerized `redis:7` |
| IBM MQ | `firn-ingest -source-ibmmq` | `firn-egress type: ibmmq` | REST endpoints via stdlib HTTP (no C client) | dockerized `mq` if the trial image runs here; else protocol-tested only |

IBM MQ deliberately uses the server's **mqweb REST API** rather than `mq-golang` (which requires the IBM MQ C client installed). Throughput is lower; strict fidelity to MQMD headers/RFH2 is out of scope. The escape hatch (`ibmmq` native client through mq-golang + MQ C client) is a documented follow-up for high-volume shops.

### 3. HTTP-family sinks via stdlib (no new dependencies)

- **Elasticsearch/OpenSearch sink** (`type: elasticsearch`): `_bulk` API, one bulk per batch, `Idempotency-Key` = batch range, configurable `index`, `pipeline`.
- **ClickHouse sink** (`type: clickhouse`): native HTTP interface `INSERT INTO ... FORMAT JSONEachRow` (+ `database`, `table`); no native TCP client.

### 4. Explicitly deferred (named, not implied)

- Deep CDC per dialect (MySQL binlog via `go-mysql`, SQL Server CDC query, Oracle LogMiner/XStream): each is a large, distinct project; the polling watermark pattern covers migration baselines today; native CDC lands as follow-up ADRs if/when demanded with concrete dialects.
- IBM MQ native (C client) path.
- Cloud-warehouse native sinks: Snowflake, BigQuery, Redshift (no local service; each deserves its own ADR + credentials model review).
- Cassandra/HBase/Couchbase/Neo4j/ClickHouse-native/Greenplum/Teradata/Netezza/Vertica/SAP HANA/DB2: **DB2** is wire-family-close to Postgres? No — DB2 has no stable pure-Go driver (`ibmdb/go_ibm_db` is cgo). Deferred until demanded with a driver approval; polling would need a cgo driver and license check. IBM DB2 via ODBC gateway is the honest alternative.
- Redis keyspace notifications/pubsub as source (streams cover the migration shape).
- Kafka Connect wire compatibility (explicitly out — ADR-001/002).

### 5. Why the split (in/out)

**In (source)** favors completeness over parallelism: one polling connector, all dialects.
**Out (sink)** favors dialect-specific conflict/upsert syntax because silent data loss is unacceptable; append-only is always the fallback.

## Consequences

- Two new generic engines (`SqlPollingConnector`, `SqlSink`) cover the long tail of relational systems.
- Four messaging systems get symmetric in/out coverage with live tests.
- Migration playbooks (Oracle→Postgres, SQL Server→ClickHouse, etc.) become copy-paste configs, documented in `docs/migration-playbooks.md`.
- IBM MQ is usable without the IBM C client (via REST), with documented throughput limits.
- Every new dependency (`go-sql-driver/mysql`, `microsoft/go-mssqldb`, `sijms/go-ora/v2`, `modernc.org/sqlite`, `rabbitmq/amqp091-go`, `nats-io/nats.go`, `redis/go-redis/v9`) is recorded here; anything beyond this list needs its own ADR.

## Implementation notes (2026-11-19): delivered with honest verification labels

Plan 010 shipped the full matrix in one slice (2026-11-19):

- **SQL source** (`firn-ingest -source-sql`): t/watermark `int64` and `timestamp` both verified (resume at midpoint, drain); identifier validation rejects injection; MariaDB 11 & Oracle Free 23ai verified for upsert path; SQL Server placeholders are `@pN` — builder correct, runtime untested here (emulation). Oracle MERGE emits `FROM dual` (≤19c compat) and parenthesized ON (required by Oracle).
- **SQL sink**: batch transaction, dialect-aware upserts; key_columns required for upsert; `append_only` escape hatch.
- **RabbitMQ/NATS/Redis Streams**: all live-verified here (5672, 4222, 6379).
- **IBM MQ**: REST contract only — IBM image (icr.io/ibm-messaging/mq) needs x86-64-v3 and does not run on this arm64 host; stub-tested request shapes only.
- **Elasticsearch / ClickHouse**: live-verified (9200, 8123).
- Elasticsearch sink is bearer-only (no signing); ClickHouse sends X-ClickHouse-User/Key when configured.
