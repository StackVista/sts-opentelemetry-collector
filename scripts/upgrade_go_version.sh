#!/usr/bin/env bash

set -euo pipefail

NEW_GO_VERSION=$1

if [ -z "$NEW_GO_VERSION" ]; then
  echo "Usage: $0 <new_go_version>"
  exit 1
fi

echo "Upgrading Go version to $NEW_GO_VERSION in all go.mod files..."

find . -type f -name "go.mod" | while read -r gomod_file; do
  echo "Processing $gomod_file..."
  module_dir=$(dirname "$gomod_file")
  (cd "$module_dir" && go mod edit -go="$NEW_GO_VERSION" && go mod tidy)
done

echo "Upgrading Go version to $NEW_GO_VERSION in go.work file..."
go work edit -go="$NEW_GO_VERSION"

echo "Go version upgrade complete."
