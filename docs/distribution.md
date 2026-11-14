# Distribution & release hardening (ADR-011, slice 6)

Releases of Firn ship as reproducible artifacts attached to GitHub releases — never as a `git clone` and hope. This page describes what the release pipeline guarantees and what the operator must do manually.

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
5. Creates the GitHub release with notes taken from `CHANGELOG.md`. Re-running on an existing release re-uploads assets with `--clobber`.

The workflow has `contents: write` only (creating the release and uploading assets). It does **not** sign artifacts — see below. The `distribution` job in `ci.yml` continues to run the dependency gate (SBOM + grype critical) on every push to `main`.

## What CI does **not** do (and why)

- **Cosign sign-blob** remains a human step (see below), deliberately: wiring `id-token: write` inside the workflow without an owner-owned OIDC identity / trust policy is a security posture we decline to take unilaterally. Artifacts upload unsigned; operators who need signed provenance should verify SHA256SUMS and, once signatures are produced, sign the release tarballs themselves.
- **SLSA attestation of the build pipeline itself**. Goreleaser-style attestation would require importing goreleaser; that's an ADR of its own.

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

1. `sha256sum -c SHA256SUMS` from the tarball attachment
2. `cosign verify-blob --key <public-key> --signature <artifact>.sig <artifact>`
3. Inspect `sbom.spdx.json` (`spdx-tools` or `jq`): expect golang + grpc + prometheus client
4. Bootstrap a fresh broker in an isolated network segment **without** mesh CA yet; observe that MTLS peers are rejected on the mesh listener (matches F-2)
5. Run `firn-cli list-apikeys` against the bootstrapped instance; expect refusal of anonymous traffic
6. Wipe scratch dirs

## What we explicitly **did not** add

- No Cosign in CI (no OIDC)
- No SLSA provenance of the build pipeline
- No image signing (no registry yet — future ADR)

## Additions pending, not blocking

- Key rotation strategy for the mesh CA
- Automatic dependency license checks vs LICENSE/NOTICE
