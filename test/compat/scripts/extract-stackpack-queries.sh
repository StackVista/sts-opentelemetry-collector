#!/usr/bin/env bash

# Find which shipped artifacts reference which metrics, so the contract can say who breaks
# if a metric disappears. Without this column the contract is a list of names; with it, it
# is a priority order.
#
# Settings bodies come from `sts settings describe`. `sts settings list` only returns a
# summary table, so it is used to enumerate identifiers and nothing else. MetricBinding is
# included because the Kubernetes metrics tab queries live there rather than in
# ComponentPresentation.
#
# Usage:
#   extract-stackpack-queries.sh [-o OUTPUT_CSV] [-d STACKPACK_DIR]...
#
# Output: stored_name,artifact  with one row per (metric, referencing artifact) pair.
# Also writes generated/queries.status, which records whether the extraction completed, so
# the contract can tell a confirmed absence of consumers from missing evidence.
#
# The extraction is deliberately greedy: it takes every identifier that looks like a stored
# metric name out of every PromQL string it can find. Over-reporting a consumer is harmless,
# missing one is not.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
COMPAT_DIR="$(dirname -- "${SCRIPT_DIR}")"

OUTPUT="${COMPAT_DIR}/generated/queries.csv"
STATUS="${COMPAT_DIR}/generated/queries.status"
DIRS=()
TYPES=(ComponentPresentation MetricBinding Monitor Dashboard)

usage() {
  sed -n '3,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while getopts ':o:d:h' opt; do
  case "${opt}" in
    o) OUTPUT="${OPTARG}" ;;
    d) DIRS+=("${OPTARG}") ;;
    h) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

mkdir -p "$(dirname -- "${OUTPUT}")"
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT
mkdir -p "${WORK}/bodies"

failures=0
cli_used=0

# sts settings list prints a table of type, ID, identifier, name, owner and timestamp.
# Only the identifier column is needed here; the bodies come from describe.
identifiers_for_type() {
  local type="$1"
  sts settings list --type "${type}" 2>/dev/null |
    awk -F'|' 'NR > 2 { gsub(/^[ \t]+|[ \t]+$/, "", $3); if ($3 != "" && $3 != "IDENTIFIER") print $3 }'
}

if command -v sts >/dev/null; then
  cli_used=1
  for type in "${TYPES[@]}"; do
    while IFS= read -r identifier; do
      [[ -z "${identifier}" ]] && continue
      safe="$(printf '%s' "${identifier}" | tr -c 'A-Za-z0-9._-' '_')"
      if sts settings describe --type "${type}" --identifier "${identifier}" \
        >"${WORK}/bodies/${type}__${safe}.yaml" 2>"${WORK}/describe.err"; then
        printf '%s\n' "${identifier}" >"${WORK}/bodies/${type}__${safe}.artifact"
      else
        failures=$((failures + 1))
        rm -f "${WORK}/bodies/${type}__${safe}.yaml"
        printf 'failed to describe %s %s: %s\n' "${type}" "${identifier}" \
          "$(head -n 1 "${WORK}/describe.err" 2>/dev/null)" >&2
      fi
    done < <(identifiers_for_type "${type}")
    printf 'described %s settings\n' "${type}" >&2
  done
else
  printf 'sts CLI not on PATH, skipping installed StackPacks\n' >&2
fi

# Local StackPack directories, if any were passed. Each file keeps its own identity.
for dir in "${DIRS[@]+"${DIRS[@]}"}"; do
  if [[ ! -d "${dir}" ]]; then
    printf 'not a directory: %s\n' "${dir}" >&2
    continue
  fi
  dir_count=0
  while IFS= read -r file; do
    rel="${file#"${dir}"/}"
    safe="$(printf '%s' "${rel}" | tr -c 'A-Za-z0-9._-' '_')"
    cp "${file}" "${WORK}/bodies/local__${safe}.yaml"
    printf '%s\n' "${rel}" >"${WORK}/bodies/local__${safe}.artifact"
    dir_count=$((dir_count + 1))
  done < <(find "${dir}" -type f \( -name '*.sty' -o -name '*.yaml' -o -name '*.yml' \) 2>/dev/null | sort)
  printf 'included %d files from %s\n' "${dir_count}" "${dir}" >&2
done

# Metric names in PromQL look like bare identifiers followed by {, [, ( or whitespace. The
# families are restricted to the node-agent ones so unrelated platform metrics do not enter
# the contract.
{
  printf 'stored_name,artifact\n'
  for body in "${WORK}"/bodies/*.yaml; do
    [[ -e "${body}" ]] || continue
    artifact_file="${body%.yaml}.artifact"
    [[ -f "${artifact_file}" ]] || continue
    artifact="$(cat "${artifact_file}")"
    grep -ohE '\b(kubernetes|container|system)_[a-z0-9_]+' "${body}" 2>/dev/null |
      sort -u |
      while IFS= read -r metric; do
        [[ -z "${metric}" ]] && continue
        printf '%s,%s\n' "${metric}" "${artifact}"
      done
  done
} >"${OUTPUT}"

total=$(($(wc -l <"${OUTPUT}") - 1))
printf 'wrote %s (%d metric references)\n' "${OUTPUT}" "${total}"

if [[ "${cli_used}" == "1" && "${failures}" -eq 0 ]]; then
  printf 'complete\n' >"${STATUS}"
  printf 'consumer extraction completed\n' >&2
else
  rm -f "${STATUS}"
  cat >&2 <<'EOF'

Consumer extraction did not complete, so the contract cannot tell a metric with no
consumers from one whose consumers were not looked at. priority stays unknown until this
runs to completion against an instance.
EOF
fi
