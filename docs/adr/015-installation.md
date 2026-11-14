# ADR-015: Installation as a service — OS packages, interactive installer, Kubernetes and OCI

Status: accepted · 2026-11-14 · implemented (packaging/, deploy/, .github/workflows/release.yml image job)

## Context

firn ships as two static binaries from GitHub releases, and every supported
setup on macOS/Linux so far has been "download the tarball, run `firnd` by
hand, Ctrl-C when done". That is fine for a laptop demo and useless for the
places the broker is actually wanted: a long-lived service account on a Linux
box, a launchd job on a macOS fleet host, a static pod on Kubernetes, or a
container on a Podman host. Docs described how to *build* firn, never how to
*run it*.

Goals:

1. Install firn as a managed service on the popular operating systems.
2. Ask the operator the questions the product needs answered up front —
   especially for a 3-node cluster: which nodes, which addresses, where the
   shared mesh CA comes from, where data lives — and never silently pick
   insecure defaults to fill a gap.
3. Give Kubernetes/OCI users first-class artifacts (plain YAML, a Helm
   chart, compose files, a systemd Quadlet) that deploy the same binaries.
4. Keep the fail-closed startup contract untouched: no installer path ever
   adds `-dev`, and the bootstrap-token flow is printed in the docs/NOTES of
   every path.

Non-goals (this ADR):

- A Windows-native service. Not supported in v1; Windows hosts are served by
  WSL2 or by the OCI image (`deploy/compose/compose.yaml`). ADR-001 owns
  this limit.
- `deb`/`rpm` packages and goreleaser. The installer downloads release
  tarballs with checksum verification; distro packages are additive later.
- A Kubernetes operator / CRDs. The product has fixed membership and no
  declarative cluster API; a chart and plain YAML are the ceiling.
- Air-gapped registry mirroring. Documented for the operator, not automated.

## Decision

### 3.1 Repository layout

```
packaging/Dockerfile                multi-stage image (golang:1.27-alpine → alpine, uid 10001)
packaging/install.sh                interactive installer (bash; systemd Linux + macOS launchd)
packaging/systemd/firnd.service     canonical unit for manual installs
deploy/k8s/firn-standalone.yaml     namespace/SA/svc/StatefulSet+PVC (RWO, Recreate)
deploy/k8s/firn-cluster.yaml        3-voter cluster; headless mesh svc; mesh-CA copy initContainer
deploy/helm/firn/                   Helm chart: standalone or GA cluster, PDB, NOTES.txt
deploy/compose/compose.yaml         docker/podman compose (default standalone; --profile cluster = 3 nodes)
deploy/compose/firnd.container      Podman Quadlet for rootless systemd integration
```

The image is published from the release workflow to `ghcr.io/rahulkj/firn`
(tag = the release tag + `latest`), built from the same commit as the signed
tarballs — see `docs/distribution.md`.

### 3.2 The interactive installer (`packaging/install.sh`)

Root-only bash. Steps, in order, all prompted (defaults in brackets):

1. Detect OS — Linux with systemd or macOS (else: point at WSL2/OCI and exit).
2. Binaries — use `firnd`/`firn-cli` from `PATH` if present; otherwise
   download the latest release tarball for the arch and **verify SHA256SUMS
   before installing**. (`FIRN_VERSION` env overrides the tag.)
3. Create the `firn` system user and `0700` data dir (default `/var/lib/firn`).
4. Ask: HTTP admin address (`:9460`), client gRPC address (`:9461`).
5. Ask: enable TLS on both listeners (y/N); if yes, cert/key paths (must exist).
6. Ask: **single node or 3-node cluster?**
   - Cluster: node-id (`n1`/`n2`/`n3`), peer list *including self*
     (e.g. `n1=192.0.2.11:9461,…`), then the mesh CA:
     generate a fresh one into `<data-dir>/mesh-ca` via the product's own
     `firn-cli mesh-ca init` (single-block ECDSA PEM) **or** use an existing shared CA
     via `-mesh-ca-bundle`/`-mesh-ca-key`. Copy instructions for the other
     nodes are printed in both branches.
7. Ask: retention TTL (`0` = disabled).
8. Write `/etc/firn/firnd.env` (mode 600) with `FIRND_FLAGS=…`, install the
   unit, `systemctl daemon-reload`, `enable --now`. On macOS the same flow
   generates a launchd plist with one `<string>` per flag and `launchctl load`s it.
9. Print the next three steps — the one-time **BOOTSTRAP TOKEN** is in the
   journal/log; mint the first admin key; check `/healthz` — plus the data-dir
   backup warning.

`--unattended` accepts defaults for non-interactive installs; it never
relaxes the security prompts. The installer never sets `-dev`.

### 3.3 Kubernetes deployment

Two manifests, both proven with `kubectl apply --dry-run=client` and the
Helm chart with `helm lint`/`helm template`:

- **Standalone** — StatefulSet 1/1, RWO PVC, `Recreate` strategy, headless
  probes on `/healthz`, uid 10001, `readOnlyRootFilesystem`, mem/cpu
  requests and limits. Single RWO volume means `Recreate` is mandatory.
- **Cluster (GA envelope)** — StatefulSet 3/3, headless mesh service
  (`publishNotReadyAddresses`), stable pod DNS (`firn-{0..2}.firn-mesh.<ns>.svc.cluster.local`),
  node-id derived from pod hostname matching the `-peers` map, peers passed
  as one static arg (fixed membership), mesh CA copied from a pre-created
  `firn-mesh-ca` Secret by an initContainer (CA generated outside the
  cluster, `0600`, mounted read-only), readiness gates staggered-boot
  tolerance (`failureThreshold: 6`), PDB `minAvailable: 2` (quorum majority),
  same security context. The chart renders the identical shape with a
  `required` guard that fails loudly when `mode=cluster` lacks
  `meshCA.existingSecret` — no silent discovery of a missing CA.

Node-id needs no custom image: the container runs `sh -c 'exec firnd … "$HOSTNAME" …'`,
so each pod is the correct voter with zero per-pod values injected.

### 3.4 OCI / Compose

One `compose.yaml` (works with `docker compose` and `podman compose`):
single-node default, optional `cluster` profile with three firn nodes on a
shared bridge, mesh CA via pre-provisioned flags (the CA is mounted
read-only at `/run/firn` and handed to firnd as `-mesh-ca-bundle`/
`-mesh-ca-key` — firnd *writes* its node certs into `<data-dir>/mesh-ca` at
first start, so the bundle must not live there read-only). Fail-closed
defaults preserved; `read_only: true` + `tmpfs /tmp`.
`firnd.container` is a rootless Podman Quadlet for systemd integration with
the same posture.

### 3.5 Image

`packaging/Dockerfile`: multi-stage build from `golang:1.27-alpine`,
`CGO_ENABLED=0`, stripped symbols, `alpine:3.21` runtime with only
`ca-certificates`, uid/gid 10001 `firn`, `VOLUME /var/lib/firn`,
`HEALTHCHECK` on `/healthz`. The same image is what CI pushes to GHCR on
tag push (see release.yml `images` job added in the same change) — no
divergence between "the released tarball" and "the released image".

## Consequences

- Every supported platform now answers "install as a service" in one
  documented command plus prompts, instead of tribal knowledge.
- Cluster installs force the mesh CA into existence at install time — the
  failure mode where nodes boot without peers and flap is designed out.
- The installer is a bash script and will drift worst of any artifact here;
  it is therefore small, shellchecked, and tested against the real
  `firnd -h` output (see verification notes in the commit message).
- The Helm chart owns cluster *shape*, not lifecycle: it never scales,
  never rotates the CA, never upgrades in place — full cluster restart is
  the documented upgrade path, consistent with ADR-007.
- Windows remains WSL2/OCI-only; if native service demand appears, its
  own ADR and a reviewer who runs Windows decides it.
- Air-gapped users must mirror the image and tarball themselves; the
  installers accept a local tarball path implicitly via PATH precedence.

## Alternatives considered

- **goreleaser + deb/rpm now** — rejected for v1: another toolchain to own
  for two package formats before one container image exists. Revisit when
  external users ask for distro packages.
- **A Kubernetes operator / CRD** — rejected: no declarative model needed
  for fixed-membership Raft voters; a chart + two manifests is the whole
  surface.
- **Helm as the only k8s path** — rejected: plain manifests cost nothing to
  maintain and `kubectl apply -f` must work without installing Helm.
- **Generating the mesh CA inside the installer into every node** — rejected
  for cluster mode: a single script producing one CA thrice risks drift;
  shared-CA copy (printed instructions) or explicit pre-provisioned flags
  keeps the trust root single and operator-held.
- **`-dev` bootstrap for convenience in installers** — never.

## Verification

- `shellcheck -S warning packaging/install.sh` — clean.
- Image built from a clean checkout and run: `/healthz` →
  `{"status":"ok","version":"0.5.3"}` as uid 10001, bootstrap-locked as
  expected.
- `docker compose -f deploy/compose/compose.yaml config` — valid; runtime
  path (image, ports, volumes) identical to the verified `docker run` flow.
- `kubectl apply --dry-run=client -f deploy/k8s/*.yaml` against a live
  v1.37.2 cluster — accepted (no writes).
- `helm lint` + `helm template` (standalone, cluster with
  `meshCA.existingSecret`, and cluster without it — the latter fails loudly
  with the intended message).
- Client-side schema pass of the rendered chart against the same API.
- **Live 3-node test on the production homelab cluster** (v0.5.3): namespace
  `firn`, mesh CA Secret from a locally generated CA. The first deploys
  surfaced and fixed three real defects: NFS subdir provisioner's mode-0777
  PVCs (dir-mode-check crash — fixed with an initContainer `chmod -R go-rwx`),
  multi-line YAML args mangling the flag list (`sh` swallowed the
  continuations; every pod silently booted single-node — fixed with
  single-line args), and the mesh CA loader rejecting openssl's two-block
  PEM (`EC PARAMETERS` + `EC PRIVATE KEY` — fixed by iterating all PEM
  blocks in `mustParseECKey`; regression test added). Post-fix: all three
  pods Ready, mesh mTLS, produce → seal → cross-pod fetch verified.
