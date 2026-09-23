#!/usr/bin/env bash

# Build contract.csv, the metrics contract for the node-agent migration (NA-1).
#
# Joins:
#   generated/agent-metrics.csv        metric inventory scanned from the agent source
#   mapping/supplemental-metrics.csv   names the agent assembles at runtime
#   mapping/otel-sources.csv           where each metric can come from in an OTel pipeline
#   generated/live-series.csv          real label sets, when a live dump is available
#   generated/queries.csv              which artifacts reference which metric
#   generated/queries.status           whether that extraction completed, so a confirmed
#                                      absence of consumers can be told from missing evidence
#
# The first three are offline and always present, so this script produces a useful contract
# without instance access. The last two fill the labels and consumers columns, and until they
# do the priority column stays at "unknown" rather than pretending.
#
# Usage: build-contract.sh [-o OUTPUT_CSV]

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
COMPAT_DIR="$(dirname -- "${SCRIPT_DIR}")"

OUTPUT="${COMPAT_DIR}/contract.csv"
GEN="${COMPAT_DIR}/generated"
MAP="${COMPAT_DIR}/mapping"

while getopts ':o:h' opt; do
  case "${opt}" in
    o) OUTPUT="${OPTARG}" ;;
    h) sed -n '3,18p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) exit 2 ;;
  esac
done

if [[ ! -f "${GEN}/agent-metrics.csv" ]]; then
  printf 'run extract-agent-metrics.sh first, %s is missing\n' "${GEN}/agent-metrics.csv" >&2
  exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

strip_comments() { grep -vE '^[[:space:]]*(#|$)' "$1"; }

# Inventory: scanned rows plus supplemental rows, scanned wins on conflict since it carries
# a file reference that a reviewer can check.
{
  tail -n +2 "${GEN}/agent-metrics.csv" | awk -F, '{ printf "%s,%s,%s,%s,scanned\n", $1, $2, $3, $4 }'
  strip_comments "${MAP}/supplemental-metrics.csv" | tail -n +1 |
    awk -F, 'NR > 0 && $1 != "metric" { printf "%s,%s,%s,%s,supplemental\n", $1, $2, $3, $4 }'
} | awk -F, '!seen[$1]++' | sort -t, -k1,1 >"${WORK}/inventory.csv"

strip_comments "${MAP}/otel-sources.csv" | tail -n +2 >"${WORK}/mapping.csv"

if [[ -f "${GEN}/live-series.csv" ]]; then
  tail -n +2 "${GEN}/live-series.csv" >"${WORK}/live.csv"
else
  : >"${WORK}/live.csv"
fi

if [[ -f "${GEN}/queries.csv" ]]; then
  tail -n +2 "${GEN}/queries.csv" >"${WORK}/queries.csv"
else
  : >"${WORK}/queries.csv"
fi

# Consumer evidence is only usable if the extraction ran to completion. Seeing live series
# is not enough, and neither is an empty queries.csv on its own.
consumers_complete=0
if [[ -f "${GEN}/queries.status" ]] && [[ "$(head -n 1 "${GEN}/queries.status")" == "complete" ]]; then
  consumers_complete=1
fi

awk -F, -v OFS=, -v consumers_complete="${consumers_complete}" '
  function stored(name,   s) { s = name; gsub(/[.-]/, "_", s); return s }

  function trim(s) { gsub(/^[ \t]+|[ \t]+$/, "", s); return s }

  # Glob match with specificity, so a family default can be overridden by a single name.
  function pattern_score(pat, name,   rx) {
    if (pat == name) return 1000
    if (index(pat, "*") == 0) return 0
    rx = "^" pat "$"
    gsub(/[.]/, "[.]", rx)
    gsub(/[*]/, ".*", rx)
    if (name ~ rx) return length(pat)
    return 0
  }

  FILENAME ~ /mapping\.csv$/ {
    np++
    p_pat[np] = $1; p_src[np] = $2; p_act[np] = $3; p_unit[np] = $4
    # notes may contain commas, so take everything from field 5 onwards
    # Notes may contain commas. Rejoin them with semicolons so the output stays valid CSV.
    note = trim($5)
    for (i = 6; i <= NF; i++) note = note "; " trim($i)
    p_note[np] = note
    next
  }

  FILENAME ~ /live\.csv$/ { labels[$1] = $2; next }

  FILENAME ~ /queries\.csv$/ {
    if (consumers[$1] == "") consumers[$1] = $2
    else if (index(consumers[$1], $2) == 0) consumers[$1] = consumers[$1] ";" $2
    next
  }

  FILENAME ~ /inventory\.csv$/ {
    metric = $1; check = $2; kind = $3; src = $4; origin = $5
    st = stored(metric)

    best = 0; bi = 0
    for (i = 1; i <= np; i++) {
      s = pattern_score(p_pat[i], metric)
      if (s > best) { best = s; bi = i }
    }

    otel = (bi ? p_src[bi] : "none")
    action = (bi ? p_act[bi] : "undecided")
    unit = (bi ? p_unit[bi] : "")
    note = (bi ? p_note[bi] : "no mapping rule matched")

    lbl = (st in labels ? labels[st] : "")
    cons = (st in consumers ? consumers[st] : "")

    if (cons != "") priority = "P1"
    else if (consumers_complete == 1) priority = "P3"
    else priority = "unknown"

    print metric, st, check, kind, unit, otel, action, lbl, cons, priority, origin, note
    next
  }

  END { }
' "${WORK}/mapping.csv" "${WORK}/live.csv" "${WORK}/queries.csv" "${WORK}/inventory.csv" |
  sort -t, -k1,1 >"${WORK}/rows.csv"

{
  printf 'metric,stored_name,check,kind,unit,otel_source,compat_action,labels,consumers,priority,origin,notes\n'
  cat "${WORK}/rows.csv"
} >"${OUTPUT}"

total=$(($(wc -l <"${OUTPUT}") - 1))
printf 'wrote %s (%d metrics)\n\n' "${OUTPUT}" "${total}"

printf 'by compat action\n'
awk -F, 'NR > 1 { c[$7]++ } END { for (a in c) printf "  %-16s %4d\n", a, c[a] }' "${OUTPUT}" | sort -k2 -rn

printf '\nby family\n'
awk -F, 'NR > 1 { split($1, p, "."); c[p[1]]++ } END { for (f in c) printf "  %-16s %4d\n", f ".*", c[f] }' "${OUTPUT}"

missing_labels=$(awk -F, 'NR > 1 && $8 == ""' "${OUTPUT}" | wc -l)
missing_consumers=$(awk -F, 'NR > 1 && $9 == ""' "${OUTPUT}" | wc -l)
printf '\nincomplete columns\n'
printf '  %-16s %4d (run dump-live-series.sh against an instance)\n' "labels" "${missing_labels}"
printf '  %-16s %4d (run extract-stackpack-queries.sh)\n' "consumers" "${missing_consumers}"

printf '\npriority\n'
awk -F, 'NR > 1 { c[$10]++ } END { for (p in c) printf "  %-16s %4d\n", p, c[p] }' "${OUTPUT}" | sort

if [[ "${consumers_complete}" == "0" ]]; then
  printf '\nconsumer evidence is missing, so no metric can be classified P3.\n'
  printf 'priority stays unknown until extract-stackpack-queries.sh completes.\n'
fi
