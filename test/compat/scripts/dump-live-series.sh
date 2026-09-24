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
#   STS_URL         base URL of the instance
#   STS_API_TOKEN   API token with read access to metrics
#   STS_METRICS_PATH  override the metrics API path, default /api/metrics
#   LOOKBACK        window for the series query, default 1h. Accepts a duration such as 30m,
#                   90s, 2d, or a plain number of seconds.
#
# Output: metric,labels  where labels is a semicolon separated sorted list of label keys.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
COMPAT_DIR="$(dirname -- "${SCRIPT_DIR}")"

OUTPUT="${COMPAT_DIR}/generated/live-series.csv"
METRICS_PATH="${STS_METRICS_PATH:-/api/metrics}"
LOOKBACK="${LOOKBACK:-1h}"

usage() {
  sed -n '3,21p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
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

# Parse the lookback into seconds with shell arithmetic, so the same code works on GNU and
# BSD userlands and non-default windows are honoured.
lookback_seconds() {
  local spec="$1" number unit multiplier
  number="${spec%[smhd]}"
  unit="${spec#"${number}"}"
  case "${unit}" in
    "")  multiplier=1 ;;
    s)   multiplier=1 ;;
    m)   multiplier=60 ;;
    h)   multiplier=3600 ;;
    d)   multiplier=86400 ;;
    *)   printf 'unsupported LOOKBACK unit: %s\n' "${unit}" >&2; return 1 ;;
  esac
  case "${number}" in
    ''|*[!0-9]*) printf 'LOOKBACK must start with a whole number, got: %s\n' "${spec}" >&2; return 1 ;;
  esac
  printf '%s\n' "$((number * multiplier))"
}

seconds="$(lookback_seconds "${LOOKBACK}")" || exit 1
if [[ "${seconds}" -le 0 ]]; then
  printf 'LOOKBACK must be greater than zero, got: %s\n' "${LOOKBACK}" >&2
  exit 1
fi

now="$(date -u +%s)"
start="$((now - seconds))"
end="${now}"

printf 'querying %s with a %s window (%s to %s)\n' \
  "${STS_URL%/}${METRICS_PATH}/series" "${LOOKBACK}" "${start}" "${end}" >&2

mkdir -p "$(dirname -- "${OUTPUT}")"

api() {
  local path="$1"
  shift
  curl -sS --fail-with-body \
    -H "Authorization: Bearer ${STS_API_TOKEN}" \
    -G "${STS_URL%/}${METRICS_PATH}/${path}" "$@"
}

# The three families the node-agent produces. Anything outside them is another component's
# problem and would only add noise to the contract.
printf 'metric,labels\n' >"${OUTPUT}.tmp"

for family in kubernetes container system; do
  printf 'querying %s_* series\n' "${family}" >&2

  # /series returns one object per series, so label keys are collected across all of them.
  api series \
    --data-urlencode "match[]={__name__=~\"${family}_.*\"}" \
    --data-urlencode "start=${start}" \
    --data-urlencode "end=${end}" |
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
