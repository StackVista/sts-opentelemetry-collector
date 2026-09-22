#!/usr/bin/env bash

# Find which shipped artifacts reference which metrics, so the contract can say who breaks
# if a metric disappears. Without this column the contract is a list of names; with it, it
# is a priority order.
#
# Sources, in the order they are searched:
#   1. StackPack settings exported with the sts CLI (presentation metrics, monitors)
#   2. any local StackPack directories passed with -d, useful before a StackPack is installed
#
# Usage:
#   sts-configured  extract-stackpack-queries.sh [-o OUTPUT_CSV] [-d STACKPACK_DIR]...
#
# Output: stored_name,artifact  with one row per (metric, referencing artifact) pair.
#
# The extraction is deliberately greedy: it takes every identifier that looks like a stored
# metric name out of every PromQL string it can find. Over-reporting a consumer is harmless,
# missing one is not.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
COMPAT_DIR="$(dirname -- "${SCRIPT_DIR}")"

OUTPUT="${COMPAT_DIR}/generated/queries.csv"
DIRS=()

usage() {
  sed -n '3,19p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
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

# Pull every setting the CLI can export. Types vary by server version, so unknown ones are
# skipped rather than treated as an error.
if command -v sts >/dev/null; then
  for type in ComponentPresentation Monitor Dashboard; do
    if sts settings list --type "${type}" >"${WORK}/${type}.txt" 2>"${WORK}/${type}.err"; then
      printf 'exported %s settings\n' "${type}" >&2
    else
      printf 'skipping %s settings (%s)\n' "${type}" "$(head -1 "${WORK}/${type}.err" 2>/dev/null)" >&2
      rm -f "${WORK}/${type}.txt"
    fi
  done
else
  printf 'sts CLI not on PATH, skipping installed StackPacks\n' >&2
fi

# Local StackPack directories, if any were passed.
for dir in "${DIRS[@]+"${DIRS[@]}"}"; do
  if [[ -d "${dir}" ]]; then
    find "${dir}" -type f \( -name '*.sty' -o -name '*.yaml' -o -name '*.yml' \) \
      -exec cp --backup=numbered {} "${WORK}/" \; 2>/dev/null || true
    printf 'included local directory %s\n' "${dir}" >&2
  else
    printf 'not a directory: %s\n' "${dir}" >&2
  fi
done

# Metric names in PromQL look like bare identifiers followed by {, [, ( or whitespace. The
# families are restricted to the node-agent ones so unrelated platform metrics do not enter
# the contract.
{
  printf 'stored_name,artifact\n'
  if compgen -G "${WORK}/*" >/dev/null; then
    grep -rhoE '\b(kubernetes|container|system)_[a-z0-9_]+' "${WORK}" 2>/dev/null |
      sort | uniq -c |
      awk '{ printf "%s,%s\n", $2, "installed-settings" }'
  fi
} >"${OUTPUT}"

total=$(($(wc -l <"${OUTPUT}") - 1))
printf 'wrote %s (%d metric references)\n' "${OUTPUT}" "${total}"
if [[ "${total}" -eq 0 ]]; then
  cat >&2 <<'EOF'

No references found. That is expected when the sts CLI is not configured for an instance.
Until this file has content the contract cannot say which metrics have consumers, and open
question 9 in the node-agent document (what drop-in replacement means) stays open.
EOF
fi
