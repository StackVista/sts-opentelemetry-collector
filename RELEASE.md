# Release

Create and push a Git tag matching `v*`.

## Commands

```sh
git checkout main
git tag vX.Y.Z
git push origin vX.Y.Z
```

Example:

```sh
git tag v0.0.35
git push origin v0.0.35
```

## What happens next

Pushing a `v*` tag triggers `.github/workflows/build.yaml`, which:

- builds the collector image
- runs Trivy scans
- publishes `quay.io/stackstate/sts-opentelemetry-collector:vX.Y.Z`
- publishes the multi-platform manifest

## Test tags

For a test release, use a tag matching `test-*`.

```sh
git tag test-my-release
git push origin test-my-release
```
