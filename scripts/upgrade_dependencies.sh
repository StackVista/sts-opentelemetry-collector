#!/usr/bin/env bash

set -euo pipefail

echo "Upgrading dependencies in all go.mod files..."

find . -type f -name "go.mod" | while read -r gomod_file; do
  echo "Processing $gomod_file..."
  module_dir=$(dirname "$gomod_file")
  (cd "$module_dir" && go get -u ./... && go mod tidy)
done

echo "Synchronizing go.work with all modules..."
go work sync

echo "Dependency upgrade and synchronization complete."
