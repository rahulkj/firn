# Migration guide — v0.3 → v0.4 (ADR-011 secure-by-default)

This document covers the upgrade from a default-installable v0.3.x to v0.4.x, where the daemon is fail-closed by default. Read it before restarting any v0.4.0 binary.

## TL;DR

| v0.3.x | v0.4.x |
|---|---|
| Fresh broker serves data without keys | Fresh broker **serves `/healthz` only** until a key exists |
| `/metrics` public | `/metrics` requires a viewer key (or `-metrics-auth=false`) |
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
