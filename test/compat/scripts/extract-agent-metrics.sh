#!/usr/bin/env bash

# Extract the Agent V2 metric inventory from a stackstate-agent checkout.
#
# This is the offline half of the metrics contract (see ../README.md). It produces one row
# per metric name the node-agent can emit, with the check that emits it and, where the
# source makes it unambiguous, the submission kind (gauge, rate, count, service check).
#
# Only the node-agent families are extracted: kubernetes.* (kubelet check), container.*
# (generic container check and its runtime adapters) and system.* (host checks). Names the
# agent assembles at runtime from a variable cannot be found by scanning source; those live
# in mapping/supplemental-metrics.csv instead.
#
# The output is a starting point for review, not an authority. Kind detection is a
# three-line window heuristic around each name literal, so treat "unknown" as "go and look".
#
# Usage:
#   extract-agent-metrics.sh [-a AGENT_CHECKOUT] [-o OUTPUT_CSV]
#
# Defaults: AGENT_CHECKOUT=~/projects/stackstate-agent, OUTPUT=../generated/agent-metrics.csv

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
COMPAT_DIR="$(dirname -- "${SCRIPT_DIR}")"

AGENT_CHECKOUT="${AGENT_CHECKOUT:-${HOME}/projects/stackstate-agent}"
OUTPUT="${COMPAT_DIR}/generated/agent-metrics.csv"

usage() {
  sed -n '3,21p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while getopts ':a:o:h' opt; do
  case "${opt}" in
    a) AGENT_CHECKOUT="${OPTARG}" ;;
    o) OUTPUT="${OPTARG}" ;;
    h) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

if [[ ! -d "${AGENT_CHECKOUT}/pkg/collector/corechecks" ]]; then
  printf 'not a stackstate-agent checkout: %s\n' "${AGENT_CHECKOUT}" >&2
  exit 1
fi

CHECKS_DIR="${AGENT_CHECKOUT}/pkg/collector/corechecks"
mkdir -p "$(dirname -- "${OUTPUT}")"

AWK_SCAN="${SCRIPT_DIR}/lib/scan-metrics.awk"
if [[ ! -f "${AWK_SCAN}" ]]; then
  printf 'missing %s\n' "${AWK_SCAN}" >&2
  exit 1
fi

# scan <check> <family> <implicit_prefix> <dir>...
#
# family          the metric name prefix to accept as a whole name, for example "system."
# implicit_prefix prefix to prepend to bare suffix literals inside submission helpers, used
#                 by the kubelet providers which pass suffixes to reportMetric and friends.
#                 Pass "" to disable.
scan() {
  local check="$1" family="$2" implicit="$3"
  shift 3

  find "$@" -name '*.go' ! -name '*_test.go' -print0 2>/dev/null |
    xargs -0 -r awk -v check="${check}" -v family="${family}" -v implicit="${implicit}" \
      -f "${AWK_SCAN}"
}

{
  # kubernetes.*: the kubelet check. Names appear three ways in this tree: as whole
  # literals, concatenated onto KubeletMetricsPrefix, and as bare suffixes handed to the
  # reportMetric, processContainerMetric and senderFunc helpers.
  scan kubelet 'kubernetes.' 'kubernetes.' "${CHECKS_DIR}/containers/kubelet"

  # container.*: the generic container check and the runtime adapters that feed it.
  scan container 'container.' '' \
    "${CHECKS_DIR}/containers/generic" \
    "${CHECKS_DIR}/containers/docker" \
    "${CHECKS_DIR}/containers/containerd" \
    "${CHECKS_DIR}/containers/cri"

  # system.*: the host checks. wlan and battery are laptop metrics, they cannot appear on a
  # node, so they are dropped rather than carried as noise.
  scan system 'system.' '' "${CHECKS_DIR}/system" "${CHECKS_DIR}/net" |
    grep -vE '^system\.(wlan|battery)\.'
} |
  sort -t, -k1,1 |
  awk -F, '
    BEGIN { print "metric,check,kind,source" }
    {
      m = $1
      if (!(m in seen)) {
        seen[m] = 1
        order[++n] = m
        check[m] = $2; kind[m] = $3; src[m] = $4
      } else if (kind[m] == "unknown" && $3 != "unknown") {
        kind[m] = $3; src[m] = $4
      }
    }
    END {
      for (i = 1; i <= n; i++) {
        m = order[i]
        printf "%s,%s,%s,%s\n", m, check[m], kind[m], src[m]
      }
    }
  ' >"${OUTPUT}"

total=$(($(wc -l <"${OUTPUT}") - 1))
printf 'wrote %s (%d metrics)\n' "${OUTPUT}" "${total}"
for family in kubernetes container system; do
  count=$(grep -cE "^${family}\." "${OUTPUT}" || true)
  printf '  %-13s %4d\n' "${family}.*" "${count}"
done
unknown=$(awk -F, 'NR > 1 && $3 == "unknown"' "${OUTPUT}" | wc -l)
printf '  %-13s %4d (needs a look in the source)\n' "unknown kind" "${unknown}"
