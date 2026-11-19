# Migration playbooks

Ready-to-adapt configurations for moving off legacy systems onto Firn (and onwards). Everything here is at-least-once; size for dedupe downstream (by natural keys in your sink DB or by `Idempotency-Key` on the egress side).

## 0. Common shape

All playbook recipes follow the same pattern:

```
legacy system → [source connector] → Firn topic → [pipeline] → [egress sink] → new system
```

1. Stand up `firnd`, create topics (`<source>_<entity>` naming).
2. Run `firn-ingest -source-<type> ...` as a service (systemd unit recommended).
3. Optionally transform in `firn-pipeline` (project/filter/map/enrich).
4. Run `firn-egress -config ...` to deliver to the new system.
5. Verify counts + spot-check rows; cut over consumer; decommission source connector.

Always: watermark column on the source table (`updated_at` or auto-increment `id`); declare `REPLICA IDENTITY FULL` on Postgres CDC tables; keep the legacy table write-compatible until cutover; checkpoint files live on local disk — back them up with the data.

## 1. Oracle → Postgres

Legacy Oracle table polling (no LogMiner needed for a baseline migration):

```json
// oracle-to-firn.json
{ "driver": "oracle", "dsn_env": "ORACLE_DSN", "table": "HR.EMPLOYEES",
  "watermark_column": "UPDATED_AT", "watermark_type": "timestamp", "key_columns": ["ID"], "batch": 1000 }
```
```sh
ORACLE_DSN='user/pass@127.0.0.1:1521/FREEPDB1' firn-ingest -source-sql oracle.json -topic ora_employees -checkpoint state/ora.cp -firn localhost:9461
```
Then into Postgres:
```json
// firn-to-pg.json
{ "firn": { "addr": "localhost:9461" },
  "routes": [{ "name": "to_pg", "sources": [{"topic":"ora_employees"}], "ack_policy":"after_sink_confirm",
    "sink": { "type":"sql", "driver":"pgx", "dsn_env":"PG_DSN", "table":"imported.employees", "key_columns":["id"] } }]}
```
Notes: `driver: pgx` for the target; Oracle MERGE requires `FROM dual` on <23ai (the connector emits it); poll order is stable under writes with `UPDATED_AT`; back-fill then tail (watermark continues indefinitely).

## 2. SQL Server → ClickHouse

```json
{ "driver": "mssql", "dsn_env": "MSSQL_DSN", "table": "dbo.FactSales",
  "watermark_column": "rowversion", "watermark_type": "int64", "key_columns": ["FactSalesKey"], "batch": 2000 }
```
```json
{ "sink": { "type":"clickhouse", "url":"http://127.0.0.1:8123", "database":"warehouse", "index":"fact_sales",
            "user_env":"CH_USER", "password_env":"CH_PASS" } }
```
Create the ClickHouse table first with a matching schema; JSONEachRow maps by column name. `rowversion` is the ideal SQL Server watermark (auto-increments on every write).

## 3. MySQL/MariaDB → Elasticsearch

```sh
MSQL_DSN='user:pass@tcp(127.0.0.1:3306)/shop' firn-ingest -source-sql mysql.json -topic shop_products -checkpoint cp/shop.cp
```
```json
{ "sink": { "type":"elasticsearch", "url":"http://localhost:9200", "index":"products",
            "auth":"bearer_env", "auth_env":"ES_TOKEN", "pipeline":"geoip" } }
```
Elasticsearch `_bulk` writes; `_id` set from record key when present; idempotent re-indexing by design.

## 4. RabbitMQ → Firn → anything

```json
{ "amqp": { "url_env":"AMQP_URL", "amqp_queue":"legacy.orders", "prefetch":64 } }
```
```sh
AMQP_URL='amqp://user:pass@127.0.0.1:5672/%2f' firn-ingest -source-amqp amqp.json -topic legacy_orders -checkpoint cp/rmq.cp
firn-egress -config firn-to-kafka.json   # or sql/elasticsearch/s3/clickhouse/...
```
Source acks only after Firn accepts. Single consumer per queue is required for ordered replay.

## 5. IBM MQ → Firn

```sh
MQ_URL='https://mq.internal:9443/ibmmq/rest/v3' firn-ingest -source-ibmmq mq.json -topic mq_events -checkpoint cp/mq.cp
```
```json
{ "ibmmq": { "url":"https://mq.internal:9443/ibmmq/rest/v3", "qmgr":"QM1", "ibmmq_queue":"LEGACY.QUEUE",
             "user":"app", "pass_env":"MQ_PASS", "ca_env":"MQ_CA_PATH" } }
```
Browse (GET) → produce → consume (DELETE): exactly-once-to-Firn intent, at-least-once reality. One connector per queue. Throughput ceiling is the REST API's (hundreds msg/s); for higher rates use the native MQ client (not shipped; see ADR-020 §4).

## 6. Redis Streams → Firn → Elasticsearch

```json
{ "redis": { "url_env":"REDIS_URL", "stream":"legacy.events", "group":"firn-legacy" } }
```

## 7. Postgres CDC (the gold path when you can get it)

When `wal_level=logical` and a slot is available, prefer the CDC path — full row images with `REPLICA IDENTITY FULL`, no watermark polling:
```sh
PG_CDC_DSN='postgres://user:pass@host/db?sslmode=disable' firn-ingest -source-postgres pg.json -topic pg_orders -checkpoint cp/pg.cp
```
Then the same egress/pipeline fan-out. Insert/update/delete all flow as events with `op: c|u|d`.

## 8. Cutover checklist

1. Baseline counts (source table / target index) match within an agreed window.
2. Watermark chaser verified: connector lag = 0 over a full business cycle.
3. DLQs empty or triaged.
4. Consumers switched to Firn-fed topic; legacy writes frozen or dual-written for one cycle.
5. Decommission source connector; keep checkpoints for audit replay.

## 9. What is NOT covered yet (be honest in the proposal)

- Native DB2, Informix, Sybase, Teradata, Vertica, Snowflake/BigQuery/Redshift direct drivers.
- IBM MQ native (mq-golang) at scale — REST only today; live-verification pending arm64/x86 hardware.
- SQL Server under emulation — same code path as Oracle's MERGE; marked stub-tested.
- Exactly-once delivery to sinks — always at-least-once + sink-side idempotency (unique keys, `_id`, Idempotency-Key).
