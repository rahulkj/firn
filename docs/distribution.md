# Distribution & release hardening (ADR-011, slice 6)

Releases of Firn ship as reproducible artifacts with Sigstore-backed build provenance attached to GitHub releases — never as a `git clone` and hope. This page describes what the release pipeline guarantees and what the operator must do manually.

**Container image**: every `v*` tag additionally publishes `ghcr.io/rahulkj/firn:vX.Y.Z` (and `:latest`), multi-arch (linux/amd64, linux/arm64), built from the same tagged tree via `docker/build-push-action` (see the `images` steps in `release.yml`). The image is the same static binary as the tarballs (uid 10001, healthz `HEALTHCHECK`). Pull it with the deployment manifests in `deploy/` (see ADR-015).

## What CI gives you on every push to `main`

| Step | Tool | Output |
|---|---|---|
| Build | `go build -trimpath -ldflags "-s -w"` | `dist/firnd`, `dist/firn-cli` (stripped static ELF) |
| SBOM | `anchore/sbom-action@v0` | `dist/sbom.spdx.json` — committed to CI artifacts |
| Vulnerability scan | `anchore/scan-action@v5` (`fail-build: true`, severity ≥ critical) | fail-fast on CVE in shipped binary dependencies |

## What CI does on a tag push (`.github/workflows/release.yml`)

Tagging `v*` (or running the **release** workflow manually via `workflow_dispatch` with a `tag` input, e.g. to re-cut an existing tag) produces the release automatically:

1. Guards that the tag matches the `version` const in `cmd/firnd/main.go` (refuses to release a mismatched tag) and that `CHANGELOG.md` has a `## <tag>` section (that section becomes the release notes).
2. Builds static `firnd`/`firn-cli` for `darwin-arm64`, `darwin-amd64`, `linux-amd64`, `linux-arm64` (`CGO_ENABLED=0`, `-trimpath -ldflags "-s -w"`).
3. Packages `firn-vX.Y.Z-<target>.tar.gz` (top-level `firnd`, `firn-cli`, `README.md`, `LICENSE` — exactly the layout `docs/operator.md` tells operators to extract) and generates a `SHA256SUMS` over the tarballs.
4. Generates `dist/sbom.spdx.json` via anchore/sbom-action.
5. **Attests build provenance** via `actions/attest@v4` with `subject-checksums: dist/SHA256SUMS` — a [SLSA v1.0 build provenance](https://slsa.dev/spec/v1.0/provenance) predicate bound to each tarball's SHA-256, signed with a short-lived Sigstore certificate minted from this workflow's OIDC identity (public repo ⇒ public good Sigstore instance). The attestation is persisted to the repository attestation store and the Sigstore bundle (`provenance.sigstore.jsonl`) is attached to the release for offline verification.
6. Creates the GitHub release with notes taken from `CHANGELOG.md`. Re-running on an existing release re-uploads assets with `--clobber`.

The workflow has `contents: write` (release + asset upload), `id-token: write` (Sigstore certificate), `attestations: write` (persist the attestation), and `artifact-metadata: write` (artifact storage metadata). Cosign sign-blob remains a human step (see below). The `distribution` job in `ci.yml` continues to run the dependency gate (SBOM + grype critical) on every push to `main`.

## Verifying build provenance (SLSA v1.0)

```sh
# Verify a tarball against the repository attestation store:
gh attestation verify firn-vX.Y.Z-linux-amd64.tar.gz --repo rahulkj/firn

# Or fully offline, using the bundle attached to the release:
gh release download vX.Y.Z --repo rahulkj/firn --pattern 'firn-vX.Y.Z-*.tar.gz' --pattern 'provenance.sigstore.jsonl'
gh attestation verify firn-vX.Y.Z-linux-amd64.tar.gz \
  --bundle provenance.sigstore.jsonl --repo rahulkj/firn

# Expected: a "valid signature" line naming the workflow that built it,
# .github/workflows/release.yml, signed by GitHub's Sigstore instance.
```

The attestation binds: artifact digest ⇒ builder (this repo, `release.yml`), the source ref **as recorded by the workflow run's OIDC identity**, and the workflow run that produced it. It does **not** certify the contents are malware-free; it certifies they were built by this pipeline from this source.

**Caveat — v0.5.0 specifically**: v0.5.0 was tagged before `release.yml` existed, so its assets were re-built and attested via a `workflow_dispatch` run (`runs/43620737740`). Its provenance identity is therefore `release.yml@refs/heads/main @ 3e8cc2d` (the workflow run context), while the binaries were built from the `v0.5.0` tag checkout — the tag tree differs from main only in CI/docs/CHANGELOG (no Go source). **v0.5.1** is the first release cut through the tag-push path (the workflow file is part of the tag tree), so its attestation identity is `refs/tags/v0.5.1` exactly. To tie tarballs back to the source for v0.5.0, check that the tag points at the commit you expect and that `SHA256SUMS` matches the downloads; for tag-ref provenance use v0.5.1+.

## What CI does **not** do (and why)

- **Cosign sign-blob** remains a human step (see below), deliberately: cosign certifies with a long-lived offline key an operator controls; the provenance attestations cover the "built by this pipeline from this source" claim, and remain valid for anyone verifying without that key. If you hold the vault key, sign too; it's belt and suspenders.
- **SLSA L3 isolated builders** (slsa-github-generator's generic builder) are not wired: the build runs inside this repo's workflow like every other CI job. The provenance attests faithfully where the build ran; it does not claim isolation from the runner. Upgrading to a hermetic builder would be its own ADR.

## Manual post-release steps (owner, optional)

```sh
# 1) Download the release assets:
gh release download vX.Y.Z --repo rahulkj/firn

# 2) Sign each tarball with cosign (offline key stored in operator's vault):
for f in firn-vX.Y.Z-*.tar.gz; do
  cosign sign-blob --key <path-to-key> --output-signature "$f.sig" "$f"
done

# 3) Upload the signatures:
gh release upload vX.Y.Z firn-vX.Y.Z-*.tar.gz.sig --clobber

# 4) (Optional) keyless attestation, only with an OIDC identity you control:
cosign sign-blob --keyless --output-signature vX.Y.Z.sig firn-vX.Y.Z-linux-amd64.tar.gz
```

## Operator's verification checklist (before upgrade in any environment)

1. `shasum -a 256 -c SHA256SUMS` from the download directory
2. `gh attestation verify firn-vX.Y.Z-linux-amd64.tar.gz --repo rahulkj/firn` (or `--bundle provenance.sigstore.jsonl` for offline verification)
3. (Optional, if the operator signed) `cosign verify-blob --key <public-key> --signature <artifact>.sig <artifact>`
4. Inspect `sbom.spdx.json` (`spdx-tools` or `jq`): expect golang + grpc + prometheus client
5. Bootstrap a fresh broker in an isolated network segment **without** mesh CA yet; observe that MTLS peers are rejected on the mesh listener (matches F-2)
6. Run `firn-cli list-apikeys` against the bootstrapped instance; expect refusal of anonymous traffic
7. Wipe scratch dirs

## What we explicitly **did not** add

- No cosign keyless in CI (the repo attestations use GitHub's Sigstore flow; self-signed keyless cosign with your own identity is still out of scope)
- No hermetic/isolated SLSA builder (the attestation records the real build environment)
- No image signing (no registry yet — future ADR)

## Additions pending, not blocking

- Key rotation strategy for the mesh CA
- Automatic dependency license checks vs LICENSE/NOTICE
