#!/usr/bin/env bash
# Regenerates ../golden.json and ../golden_tags.json from the cluster agent's kubernetes_state_core check.
# Usage: generate.sh <stackstate-agent checkout>
# Runs in a temporary worktree of the checkout's HEAD; the checkout is not modified.
set -euo pipefail
agent=${1:?stackstate-agent checkout required}
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
cleanup() {
  git -C "$agent" worktree remove --force "$work/agent" 2>/dev/null || true
  rm -rf "$work"
  git -C "$agent" worktree prune
}
trap cleanup EXIT
git -C "$agent" worktree add --quiet --detach "$work/agent" HEAD
cp "$here/golden_harness_test.go.txt" "$work/agent/pkg/collector/corechecks/cluster/ksm/zz_ksm_golden_test.go"
cp "$here/golden_apiclient.go.txt" "$work/agent/pkg/util/kubernetes/apiserver/zz_golden_apiclient.go"
run() {
  (cd "$work/agent" && GOLDEN_FIXTURE="$here/../objects.json" GOLDEN_CONFIG="$1" GOLDEN_OUTPUT="$2" \
    go test -tags kubeapiserver,test -run TestGenerateKSMGolden -count=1 ./pkg/collector/corechecks/cluster/ksm/)
}
run "" "$here/../golden.json"
run "$here/../golden_tags_config.json" "$here/../golden_tags.json"
echo "golden files generated from stackstate-agent $(git -C "$agent" rev-parse --short HEAD)"
