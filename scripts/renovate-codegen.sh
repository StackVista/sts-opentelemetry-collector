#!/usr/bin/env bash
# Runs upgraded generator code in a container without credentials, .git or host mounts.
set -euo pipefail

outputs=(
  extension/settingsproviderextension/generated/settingsproto/model.gen.go
  extension/settingsproviderextension/generated/settingsschema/model.gen.go
  go.work.sum
)
go_version=$(sed -n 's/^go \([0-9][0-9.]*\)$/\1/p' go.work)
name="renovate-codegen-$$"
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT

git ls-files -z | tar --null -T - -cf - | timeout 600 docker run -i --name "$name" \
  --cap-drop=ALL --security-opt=no-new-privileges --env GOTOOLCHAIN=local \
  "registry.suse.com/bci/golang:${go_version}" sh -euc '
    mkdir /source && cd /source && tar --no-same-owner -xf -
    go -C extension/settingsproviderextension generate ./generated/settingsproto/... ./generated/settingsschema/...' >&2

# -L copies link targets as regular files, so the container cannot plant symlinks here.
for f in "${outputs[@]}"; do
  docker cp -L "$name:/source/$f" "$f"
done
