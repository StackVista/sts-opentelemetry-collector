#!/usr/bin/env bash
# Regenerates ../golden.json from the cluster-agent collectors.
# Usage: generate.sh <stackstate-agent checkout>
set -euo pipefail
agent=${1:?stackstate-agent checkout required}
here=$(cd "$(dirname "$0")" && pwd)
target="$agent/pkg/collector/corechecks/cluster/topologycollectors/zz_exporter_golden_test.go"
cp "$here/golden_harness_test.go.txt" "$target"
trap 'rm -f "$target"' EXIT
(cd "$agent" && GOLDEN_FIXTURE="$here/../cluster.json" GOLDEN_OUTPUT="$here/../golden.json" \
  go test -tags kubeapiserver,test -run TestGenerateExporterGolden -count=1 ./pkg/collector/corechecks/cluster/topologycollectors/)
echo "golden.json generated from agent $(git -C "$agent" rev-parse --short HEAD)"
