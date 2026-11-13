# License & IP audit — 2026-11-13

**Author: engineering evidence, not legal advice.** This document records the
technical audit performed before cutting `v0.2.0` under the Slipway brand. It
is not a legal opinion and creates no attorney–client relationship. Before any
public distribution, have counsel review this memo and the repository.

## Scope

- Does this codebase incorporate (copy, adapt, or derive) code from Apache
  Kafka (Apache-2.0) or RabbitMQ (MPL-2.0 / Apache-2.0 components)?
- Are we encumbered by their licenses (notice preservation, copyleft,
  trademark restrictions)?
- Does the product use their marks in a way that implies endorsement or affiliation?

## Method

1. Dependency review — all direct dependencies and the full module graph (`go list -m all`).
2. Tree-wide searches for foreign license/copyright markers, SPDX headers, and
   distinctive identifiers from either codebase (e.g. `isr`, `in_sync_replica`,
   AMQP exchange types, `basic.ack`, `num.partitions`, `amqp091`).
3. Nominative-use scan — product-facing references to either project's names. Zero remain after the Slipway rename.

## Findings

### 1. No code from either project is incorporated

Direct dependencies only:

| Module | Version | License |
|---|---|---|
| `github.com/prometheus/client_golang` | v1.25.0 | Apache-2.0 |
| `google.golang.org/grpc` | v1.85.0 | Apache-2.0 |
| `google.golang.org/protobuf` | v1.36.12 | BSD-3-Clause |

Transitive dependencies (prometheus ecosystem, gRPC/protobuf tooling,
opentelemetry detectors, golang.org/x) are likewise unrelated to Apache Kafka
and RabbitMQ. No AMQP or Kafka client library, broker, or test-harness module
is present anywhere in the graph. Nothing is vendored in-tree.

License-marker scan over all tracked files: **zero** hits for SPDX headers,
`Copyright` lines, `redistribution` grants, or foreign license references. All
code is original to this repository, written from the specifications in
`docs/adr/` (ADR-003 frame format, ADR-005 scope, etc.).

### 2. License obligations: none from those projects

- Apache Kafka: Apache License 2.0 requires NOTICE preservation and license
  inclusion **only for derived works**. No derived works here → no notice
  obligation, no patent-grant issue.
- RabbitMQ: Mozilla Public License 2.0 applies at the **file level** to files
  that are MPL-covered. No MPL-covered file is copied or adapted here → no
  source-disclosure obligation is triggered.
- We do not redistribute, link, or statically embed either broker.

### 3. Trademark / nominative use

- The rename to **Slipway** removed every product-facing reference to either
  project (README, AGENTS.md, ADRs, plans, docs, comments). Zero occurrences
  remain in the repo.
- Shared vocabulary (`consumer group`, `generation`, `heartbeat`, `acks=1`,
  `session_timeout_ms`, `__consumer_offsets`) is industry-standard terminology
  for broker concepts, not protected expression; our implementations are
  original text and original machinery.
  - Note: `__consumer_offsets` matches the internal-topic name used by one
    incumbent broker; it is a descriptive name for "where committed consumer
    offsets live", deliberately identical in role (not in content) to make
    the concept legible to operators. If you want strictly zero naming overlap,
    rename the topic to `__group_offsets` everywhere (docs, tests, plans, code) —
    tracked as an optional follow-up.
- We make **no compatibility or affiliation claims** to either project; the
  docs explicitly state there is no wire compatibility with any other broker.

### 4. Provenance of this audit itself

The searches above were executed against the working tree on 2026-11-13; the
dependency graph was captured via `go list -m all` on go1.27.4. Re-run after
any dependency change.

## Gaps found (not Kafka/RabbitMQ related, but blocking distribution)

1. **This repository has no LICENSE file.** A project handing out binaries and
   source to the public with no license grants is legally unusable by its
   recipients (no rights to copy/modify/redistribute). Choose a license before
   publishing — permissive options used by comparable projects are MIT or
   Apache-2.0; the author must pick (deliberately not chosen here).
2. **"Slipway" has not been cleared through USPTO/EUIPO.** It is an ordinary
   English word (a nautical launching ramp); using an ordinary-word mark in
   software is generally permissible but clearance is counsel's call, not ours.
3. CHANGELOG history retains the old project name and pre-rename references to
   the incumbent projects by necessity (it records what the v0.1.0/v0.2.0
   releases actually were). Historical references of that kind are nominative
   and factual; they are not part of the product's branding.

## Recommended actions

1. Add `LICENSE` (owner decision; suggest MIT or Apache-2.0) and reference it
   in `README.md`.
2. Run a proper trademark clearance for "Slipway" before a public launch.
3. Re-run this audit whenever `go.mod` changes; add a license-check note to CI.
4. Have counsel review this document and the repository before any public
   release beyond an internal or private preview.
