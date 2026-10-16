#!/usr/bin/env bash
# Regenerates ../golden.json from the cluster agent's kubernetes_state_core check.
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
(cd "$work/agent" && GOLDEN_FIXTURE="$here/../objects.json" GOLDEN_OUTPUT="$here/../golden.json" \
  go test -tags kubeapiserver,test -run TestGenerateKSMGolden -count=1 ./pkg/collector/corechecks/cluster/ksm/)
echo "golden.json generated from stackstate-agent $(git -C "$agent" rev-parse --short HEAD)"
