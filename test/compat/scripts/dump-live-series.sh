#!/usr/bin/env bash

# Dump the Agent V2 series that a live SUSE Observability instance actually stores, with
# their label sets. This is the half of the metrics contract that cannot be derived from
# source: it answers what the stored names really are and, more importantly, what the label
# keys really are (see the label spelling question in the node-agent document, section 3.1).
#
# Requires an instance that is still receiving data from an Agent V2 node-agent.
#
# Usage:
#   STS_URL=https://your-instance STS_API_TOKEN=... dump-live-series.sh [-o OUTPUT_CSV]
#
#   STS_URL        base URL of the instance
#   STS_API_TOKEN  API token with read access to metrics
#   STS_PROMQL_PATH  override the PromQL API path, default /api/promql/api/v1
#   LOOKBACK       PromQL lookback window for the series query, default 1h
#
# Output: metric,labels  where labels is a semicolon separated sorted list of label keys.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
COMPAT_DIR="$(dirname -- "${SCRIPT_DIR}")"

OUTPUT="${COMPAT_DIR}/generated/live-series.csv"
PROMQL_PATH="${STS_PROMQL_PATH:-/api/promql/api/v1}"
LOOKBACK="${LOOKBACK:-1h}"

usage() {
  sed -n '3,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while getopts ':o:h' opt; do
  case "${opt}" in
    o) OUTPUT="${OPTARG}" ;;
    h) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

for dep in curl jq; do
  command -v "${dep}" >/dev/null || { printf 'missing dependency: %s\n' "${dep}" >&2; exit 1; }
done

: "${STS_URL:?set STS_URL to the instance base URL}"
: "${STS_API_TOKEN:?set STS_API_TOKEN to an API token with metric read access}"

mkdir -p "$(dirname -- "${OUTPUT}")"

api() {
  local path="$1"
  shift
  curl -sS --fail-with-body \
    -H "Authorization: Bearer ${STS_API_TOKEN}" \
    -G "${STS_URL%/}${PROMQL_PATH}/${path}" "$@"
}

# The three families the node-agent produces. Anything outside them is another component's
# problem and would only add noise to the contract.
printf 'metric,labels\n' >"${OUTPUT}.tmp"

for family in kubernetes container system; do
  printf 'querying %s_* series\n' "${family}" >&2

  # /series returns one object per series, so label keys are collected across all of them.
  api series \
    --data-urlencode "match[]={__name__=~\"${family}_.*\"}" \
    --data-urlencode "start=$(date -u -d "-${LOOKBACK}" +%s 2>/dev/null || date -u -v-1H +%s)" \
    --data-urlencode "end=$(date -u +%s)" |
    jq -r '
      .data // []
      | group_by(.__name__)
      | map({
          metric: .[0].__name__,
          labels: (map(keys) | add | unique | map(select(. != "__name__")) | sort | join(";"))
        })
      | .[] | [.metric, .labels] | @csv
    ' |
    tr -d '"' >>"${OUTPUT}.tmp"
done

sort -u -t, -k1,1 -o "${OUTPUT}.tmp" "${OUTPUT}.tmp"
{
  printf 'metric,labels\n'
  grep -v '^metric,labels$' "${OUTPUT}.tmp" || true
} >"${OUTPUT}"
rm -f "${OUTPUT}.tmp"

total=$(($(wc -l <"${OUTPUT}") - 1))
printf 'wrote %s (%d stored series names)\n' "${OUTPUT}" "${total}"

# The label spelling question is worth answering loudly, since two migrations depend on it.
printf '\nlabel keys seen on container level series:\n'
awk -F, 'NR > 1 && $1 ~ /^(kubernetes|container)_/ { print $2 }' "${OUTPUT}" |
  tr ';' '\n' | sort | uniq -c | sort -rn | head -20
