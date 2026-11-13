# Federation cutover: passive sink → writable cluster

Audience: operator of a one-way (passive) federation pair who wants to start writing on the sink. Precondition: the sink has imported the source fully and serves reads. This is an **operator procedure, not automatic**; nothing in firn flips modes for you.

## Preconditions

1. **Lag is zero.** On the sink: `firn-cli federation status` shows `last_source_offset` equal to the source's current HWM for every replicated topic/partition; `firn_federation_cursor_lag_seconds` ≈ 0; `/ui/federation` shows cursors within a few seconds of real time.
2. **Partition counts match** (source and sink must agree) — if not, the import refuses to advance; fix configs first.
3. **`__queue_attempts` state is intact.** The sink holds a byte-identical mirror of the source's attempts — do **not** wipe it or attempt counts reset (jobs may redeliver/DLQ). If the sink previously served queue traffic locally for a federated queue, it needs an explicit reseed, not a "just flip the flag" moment.

## Cutover

Pick one:

**A. Flip to active-active (both clusters keep accepting writes):**

```sh
firn-cli -http-addr <sink-http> -api-key <sink-admin> federation add-peer \
  --name <peer> --addr <source-mesh-addr> --server-name <source-id> \
  --direction both --mode active-active --topics <topic> --key <source-issued-key>
```

- The sink stays fenced for the mirrored topics until no passive-in peer covers them — so first delete the passive registration (`DELETE /federation/peers/<peer>` on the sink) or overwrite it as shown above with mode `active-active`.
- The two clusters then diverge in offset space; records replicate both ways re-origined. Cross-cluster global ordering does not exist.

**B. Decommission the federation (sink becomes standalone):**

```sh
firn-cli -http-addr <sink-http> -api-key <sink-admin> federation remove-peer --name <peer>
```

- The sink stops importing; it is immediately writable (its fence lifts).
- Its offset space continues from where it was — for topics created with the same name. If you later re-register it as a sink of the same source, expect `__federation_peers`-tracked cursors to have `last_source_offset` already at the old position; a full re-import requires peer deletion + optional `-segment-bytes`-style wipe of local segments first (documented reseed procedure; not automatic).

## After cutover / verification

1. `Produce` on the sink returns normal offsets at HWM+1.
2. `FetchGroupOffsets`-driven consumers keep their positions; offsets imported via federation are valid local offsets.
3. For queues: claim on the sink and expect the mirrored attempt counts to keep counting from the mirrored values (at-least-once preserved); if the sink had never claimed anything (fresh mirror), starts at zero as expected.
4. If you need failure-domain isolation back later, re-register both directions with mode `active-active` and both clusters will converge on both sets of records (with duplicates possible — dedupe happens at the queue/DLQ layer, not the log).

Not supported in v0.4.0: automatic mode flip, cross-cluster offset aliasing, "big bang" write-lock during cutover. If any of those is required, do it deliberately by taking the sink offline.
