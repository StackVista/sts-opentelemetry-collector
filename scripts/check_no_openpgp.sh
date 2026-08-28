#!/usr/bin/env bash

# Guards the GO-2026-5932 VEX statement in StackVista/vexhub, which asserts these
# binaries are not affected because they never link the deprecated
# golang.org/x/crypto/openpgp packages. golang.org/x/crypto has no release that
# fixes the advisory, so that claim is the only thing keeping this off the
# permanent-finding list — and nothing else notices when a new dependency drags
# the packages back in.
#
# Reads package paths out of the Go pclntab, which survives the `-s -w` that OCB
# links with. `go tool nm` and `govulncheck` are both unusable here: nm needs the
# stripped ELF symbol table, and govulncheck reports this advisory from the module
# version alone — it emits the same seven openpgp packages for a binary that
# imports nothing but chacha20.
#
# Fail-closed: absence of matches is only meaningful once we know x/crypto package
# paths are visible at all, so a binary yielding no x/crypto paths whatsoever
# fails rather than passing on missing evidence.

set -euo pipefail

readonly MODULE_PREFIX='golang.org/x/crypto/'
readonly VULNERABLE_PREFIX='golang.org/x/crypto/openpgp'

if [ "$#" -eq 0 ]; then
  echo "usage: ${0##*/} <binary>..." >&2
  exit 2
fi

status=0

for binary in "$@"; do
  if [ ! -f "$binary" ]; then
    echo "FAIL ${binary}: not a file" >&2
    status=1
    continue
  fi

  packages=$(grep -aoE "${MODULE_PREFIX}[A-Za-z0-9_/]*" "$binary" | sort -u || true)

  if [ -z "$packages" ]; then
    echo "FAIL ${binary}: no ${MODULE_PREFIX} paths found, so an empty openpgp result proves nothing" >&2
    status=1
    continue
  fi

  if offenders=$(grep -F -- "$VULNERABLE_PREFIX" <<<"$packages"); then
    echo "FAIL ${binary}: links ${VULNERABLE_PREFIX}, so the GO-2026-5932 not_affected claim no longer holds" >&2
    sed 's/^/  /' <<<"$offenders" >&2
    status=1
    continue
  fi

  echo "OK ${binary}: $(wc -l <<<"$packages") x/crypto packages linked, none under ${VULNERABLE_PREFIX}"
done

exit "$status"
