# Multigres Releases

This page describes what a Multigres release contains, how to use those
artifacts directly, and how to verify the container images before running them.
For Kubernetes deployments, the normal entry point is the Multigres Operator.

## Release Artifacts

Each tagged Multigres core release publishes:

- source archives on the GitHub release
- downloadable binary archives for `linux` and `darwin`, on `amd64` and `arm64`
- a `checksums.txt` file for downloadable binary archives
- versioned container images on GitHub Container Registry
- SBOM, provenance, and signature metadata for release container images

Downloadable binary archives include:

- `multigres`
- `pgctld`
- `multigateway`
- `multiorch`
- `multipooler`
- `multiadmin`

Container images include:

- `ghcr.io/multigres/multigres`
- `ghcr.io/multigres/pgctld`
- `ghcr.io/multigres/multiadmin-web`

`multiadmin-web` is distributed as a container image, not as a GoReleaser binary
archive. `portpoolserver` is test and development infrastructure and is not a
release artifact.

## Kubernetes Installs

For Kubernetes deployments, start with the Multigres Operator release you plan
to install. The operator release notes or install docs should say which
Multigres core image tag or digest that operator release uses.

For the alpha EKS deployment path, see
[Getting Started on EKS](docs/kubernetes/eks.md).

Direct Multigres core releases are useful for:

- inspecting or verifying published artifacts
- running binaries outside Kubernetes
- building from a tagged source release
- debugging an operator-managed deployment with matching core binaries

## Compatibility

Multigres core and Multigres Operator are versioned separately. When a release
is intended to work with a specific operator version, we document that pairing.

| Release             | Operator  | Core     | Kubernetes                 | PostgreSQL      | Primary install path                 | Status            |
| ------------------- | --------- | -------- | -------------------------- | --------------- | ------------------------------------ | ----------------- |
| Initial OSS release | `v0.11.x` | `v0.1.x` | See operator release notes | PostgreSQL 17.x | Multigres Operator release manifests | Supported release |

Combinations outside this table may work, but they are not the documented
release pairing unless a specific operator release says so.

## Build from Source

```bash
export VERSION=vX.Y.Z

git clone https://github.com/multigres/multigres.git
cd multigres
git checkout "${VERSION}"

make tools
make build-release
```

The binaries are written to `bin/`.

## Verify Binary Archives

Replace `vX.Y.Z`, `linux`, and `amd64` with the release, operating system, and
architecture you want to verify. Use `darwin` for macOS archives.

```bash
export VERSION=vX.Y.Z
export VERSION_NO_V="${VERSION#v}"
export OS=linux
export ARCH=amd64
export EXT=tar.gz
export ARCHIVE="multigres-${VERSION_NO_V}-${OS}-${ARCH}.${EXT}"

curl -fLO "https://github.com/multigres/multigres/releases/download/${VERSION}/${ARCHIVE}"
curl -fLO "https://github.com/multigres/multigres/releases/download/${VERSION}/checksums.txt"

grep "  ${ARCHIVE}$" checksums.txt | shasum -a 256 -c -
```

For macOS archives, set `OS=darwin` and `EXT=zip`.

## Verify Container Images

Release container images are signed by immutable digest using keyless
Sigstore/cosign. The images also include BuildKit provenance and SBOM
attestations tied to the published digest.

Install Docker Buildx, cosign, and jq before running the commands below.

### Inspect the Digest

```bash
export VERSION=vX.Y.Z
export IMAGE=ghcr.io/multigres/multigres

docker buildx imagetools inspect "${IMAGE}:${VERSION}"
export DIGEST="$(
  docker buildx imagetools inspect "${IMAGE}:${VERSION}" \
    --format '{{ json .Manifest }}' | jq -r .digest
)"
export IMAGE_BY_DIGEST="${IMAGE}@${DIGEST}"

echo "${IMAGE_BY_DIGEST}"
```

Use `IMAGE_BY_DIGEST` for all subsequent verification commands. Pinning by
digest avoids trusting a mutable tag after the digest has been inspected.

### Verify the Signature

```bash
cosign verify \
  --certificate-identity "https://github.com/multigres/multigres/.github/workflows/release.yml@refs/tags/${VERSION}" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  "${IMAGE_BY_DIGEST}" | jq .
```

This checks that the digest was signed by the Multigres release workflow using
the GitHub Actions identity for the release tag.

### Inspect Provenance

```bash
docker buildx imagetools inspect "${IMAGE_BY_DIGEST}" \
  --format '{{ json .Provenance }}' | jq -e '.SLSA'
```

The command should print BuildKit provenance for the signed digest. A `null` or
empty result means provenance was not found for that image digest.

### Inspect the SBOM

Inspect the full SBOM data attached to the image:

```bash
docker buildx imagetools inspect "${IMAGE_BY_DIGEST}" \
  --format '{{ json .SBOM }}' | jq -e .
```

For a platform-specific SPDX SBOM, select the platform key:

```bash
docker buildx imagetools inspect "${IMAGE_BY_DIGEST}" \
  --format '{{ json (index .SBOM "linux/amd64").SPDX }}' | jq -e .
```

Use `linux/arm64` instead of `linux/amd64` to inspect the arm64 image SBOM.
