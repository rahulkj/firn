# Distribution & release hardening (ADR-011, slice 6)

Releases of Firn ship as signed, attested artifacts — never as a `git clone` and hope. This page describes what the release pipeline guarantees and what the operator must do manually.

## What CI gives you on every push to `main`

| Step | Tool | Output |
|---|---|---|
| Build | `go build -trimpath -ldflags "-s -w"` | `dist/firnd`, `dist/firn-cli` (stripped static ELF) |
| SBOM | `anchore/sbom-action@v0` | `dist/sbom.spdx.json` — committed to CI artifacts |
| Vulnerability scan | `anchore/scan-action@v5` (`fail-build: true`, severity ≥ critical) | fail-fast on CVE in shipped binary dependencies |

## What CI does **not** do (and why)

- **Cosign sign-blob**. The repo owner cuts releases by hand today; signing is deliberately deferred to that human step and documented below, not wired into CI without an explicit OIDC identity grant. Wiring `id-token: write` inside the workflow without an owner-owned trust policy is an escalation we decline to make in this slice.
- **SLSA attestation of the build pipeline itself**. Goreleaser-style attestation would require importing goreleaser; that's an ADR of its own.

## Manual release flow (owner executes at tag time)

```sh
# 0) CI is green on the tag candidate; SBOM is present in artifacts.
# 1) Build for all targets:
for target in darwin-arm64 darwin-amd64 linux-amd64 linux-arm64; do
  goos=${target%-*}; goarch=${target#*-}
  CGO_ENABLED=0 GOOS=$goos GOARCH=$goarch \
    go build -trimpath -ldflags "-s -w" \
    -o dist/firnd-$target ./cmd/firnd
  CGO_ENABLED=0 GOOS=$goos GOARCH=$goarch \
    go build -trimpath -ldflags "-s -w" \
    -o dist/firn-cli-$target ./cmd/firn-cli
done

# 2) Sign each artifact with cosign (offline key stored in operator's vault):
for f in dist/firnd-* dist/firn-cli-*; do
  cosign sign-blob --key <path-to-key> --output-signature "$f.sig" "$f"
done

# 3) Produce tarballs + SHA256SUMS + attach SBOM:
cd dist && for f in firnd-* firn-cli-*; do tar -czf firn-vX.Y.Z-${f#firnd-}.tar.gz "$f" ../README.md; done
shasum -a 256 firn-v*.tar.gz >> SHA256SUMS

# 4) Wire attestation (optional; only if you have an OIDC exchange):
cosign sign-blob --keyless --output-signature vX.Y.Z.sig firn-vX.Y.Z.tar.gz

# 5) Create the GitHub release (requires gh CLI):
gh release create vX.Y.Z dist/firn-vX.Y.Z-*.tar.gz dist/SHA256SUMS dist/sbom.spdx.json \
  --notes-file dist/RELEASE_NOTES.md
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
