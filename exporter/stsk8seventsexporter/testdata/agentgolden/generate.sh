#!/usr/bin/env bash
# Regenerates ../golden.json from the cluster agent's kubernetes_api_events mapping.
# Usage: generate.sh <stackstate-agent checkout>
set -euo pipefail
agent=${1:?stackstate-agent checkout required}
here=$(cd "$(dirname "$0")" && pwd)
target="$agent/pkg/collector/corechecks/cluster/kubeapi/zz_events_exporter_golden_test.go"
cp "$here/golden_harness_test.go.txt" "$target"
trap 'rm -f "$target"' EXIT
(cd "$agent" && GOLDEN_FIXTURE="$here/../objects.json" GOLDEN_OUTPUT="$here/../golden.json" \
  go test -tags kubeapiserver,test -run TestGenerateEventsExporterGolden -count=1 ./pkg/collector/corechecks/cluster/kubeapi/)
echo "golden.json generated from stackstate-agent $(git -C "$agent" rev-parse --short HEAD)"
