# Migration guide — v0.3 → v0.4 (ADR-011 secure-by-default)

This document covers the upgrade from a default-installable v0.3.x to v0.4.x, where the daemon is fail-closed by default. Read it before restarting any v0.4.0 binary.

## TL;DR

| v0.3.x | v0.4.x |
|---|---|
| Fresh broker serves data without keys | Fresh broker **serves `/healthz` only** until a key exists |
| `/metrics` public | `/metrics` requires a viewer key (or `-metrics-auth=false`) |
| Default listeners `:9092 / :9093` (+mesh `:9103`) | Default listeners **`:9460 / :9461`** (mesh `grpc+10 = :9471`) — all flags unchanged, still overridable |
| Single-node `GetMetadata` advertised hardcoded `localhost:9093` | Advertises unknown (`""`); clients fall back to their seed list |
| Quorum mesh is plaintext TCP | Quorum mesh requires mTLS (cluster CA pre-provisioned) |
| No `-audit-max-bytes` | `-audit-max-bytes` (default 64 MiB) bounds the replicated `__audit` topic |
| Topic-name partitions are 0755/files 0644 | Everything is 0700/0600; `-dir-mode-check` refuses startup otherwise |
| Anonymous traffic unlimited | `-anon-rate` caps anonymous traffic per IP |

## Required steps

1. **Back up** `<data-dir>` (including mesh-ca).
2. **Provision the mesh CA once** if the cluster is multi-node:
   ```sh
   firn-cli mesh-ca init -out /secure/mesh-ca
   chmod 0600 /secure/mesh-ca/ca.*
   ```
   copy `ca.crt` and `ca.key` to every node's `<data-dir>/mesh-ca/` (mode `0600`).
3. **Mint one admin key on the first node to run v0.4.0**:
   - v0.4.0 starts with zero keys → prints `BOOTSTRAP TOKEN: <hex>` on stderr.
   - Use `curl -X POST /apikeys` with `Authorization: Bearer <token>` to create the first key.
   - Distribute the key to any operator managing the cluster.
4. **Validate** `/healthz` on every node; verify topics.rb recreate logs resume; restart peers; verify replication traces on `__consumer_offsets`.
5. **Rotate all data-dir modes**: run `chmod -R go-rwx <data-dir>` once; `firn` with `-dir-mode-check=false` only for lab/dev or exotic filesystems.

## Breaking: default listener ports changed

The v0.3.x defaults (`:9092` HTTP admin, `:9093` client gRPC, `:9103` mesh) were inherited from Kafka's de-facto port pair — unhelpful: IANA assigns 9092 to `XmlIpcRegSvc` and 9093 to `Copycat` (an embeddable Raft service), and an operator reading `strace`/firewall rules reasonably concluded "Kafka". v0.4.0 moves the defaults onto verified-unassigned ports: **`:9460` (HTTP admin), `:9461` (client gRPC), `:9471` (mesh = `grpc+10`)**. Nothing else about the listener layout changed.

| Action | Required? |
|---|---|
| Update firewalls / security groups / k8s Services to `9460`, `9461`, `9471` | Yes (unless you pinned `-http-addr`/`-grpc-addr`/`-mesh-addr`) |
| Update health-check URLs and Prometheus scrape targets (`-metrics-auth=true` by default) | Yes |
| Update `firn-cli` invocations (env defaults `FIRN_ADDR`/`FIRN_HTTP_ADDR`, or flags) | Only if you relied on the defaults |
| Update docs, client libraries, README examples, systemd unit files | Yes — the docs in this repo are already switched |
| Data directory, keys, `__consumer_offsets`, topics | Not touched |
| CLI flag names, response/wire format | Not touched |

Pin explicitly if you need to coexist with the old values temporarily: `firnd -http-addr :9092 -grpc-addr :9093` still works — the flags are unchanged; only the defaults moved.

**Note:** `docs/plans/001-mvp-single-node.md` and `docs/adr/008-documentation.md` reference the v0.1.0-era `:9092`/`:9093` as historical context; they are not current documentation.

## What you do *not* need to change

- Existing `__api_keys` are still valid — their on-disk format is identical; only file modes have tightened.
- Topics, consumer groups, queues, DLQ are unchanged — no data format changes.

## Failure modes and recovery

| Symptom | Cause | Fix |
|---|---|---|
| `mesh: no mesh CA found` | Cluster mode without provisioned CA | Provision once with `firn-cli mesh-ca init` and copy into all nodes |
| `dir ... has mode 0755` at startup | Pre-existing `0755/0644` files | `chmod go-rwx <data-dir>` recursively, or start once with `-dir-mode-check=false` and rotate; then revert |
| `:v2` connectivity fail mesh peers | Mesh client cert rejected by another node | Cluster nodes must have been provisioned with the **same** cluster CA before restart; reissue per-node certs by removing `node.crt` + `node.key` and restarting |
| `403` with valid key | Your key has a CIDR allowlist excluding the caller | Remove/adjust allowlist via `POST /apikeys/{name}` (or revoke + recreate) |
| `429` with valid key | Per-key rate limit or anonymous rate limit exceeded | Tune `-rate` / `-anon-rate` |

## Verdict on the going-away

- Nothing in v0.3 is deleted; all UX is migrated through additive flags. We did not delete any flag or API. A future v0.5 may remove `-dev`/`-allow-open-bootstrap`; do not rely on them in production.
