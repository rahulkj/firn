# ADR-001: Scope — what eventing is and is not

Status: accepted · 2026-11-13

## Context

The goal: a Go broker providing both **streaming** (Kafka-like: topics, partitions, replay, consumer groups) and **queue** (RabbitMQ-like: acknowledgements, redelivery, dead-lettering) capabilities. The naive reading — "reimplement all of Kafka and all of RabbitMQ" — is years of work and mostly protocol archaeology.

## Decision

Eventing is a **single, unified log-based broker**:

- Streaming mode: topics with N partitions; records fetched by offset; consumer groups commit offsets.
- Queue mode: the same log, consumed via `acknowledge()`; redelivery after a visibility timeout; DLQ after `maxDeliveries`.

One storage engine, one replication story (later), two API surfaces.

### Explicit non-goals for v1

Replication; exactly-once semantics/transactions; Kafka **wire protocol** compatibility; AMQP wire protocol; multi-tenancy/authZ; multi-datacenter; stream processing (KSQL/Streams). Each may be revisited as its own ADR.

## Consequences

- We optimize for correctness and operability of one storage engine, not protocol breadth.
- Client compatibility comes from SDKs we write, not wire compatibility.
- "Kafka AND RabbitMQ" in v1 means *semantics* parity within one deployment, not two brokers in a trench coat.
