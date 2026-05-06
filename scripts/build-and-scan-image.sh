#!/usr/bin/env bash

set -euo pipefail

DOCKERFILE="${DOCKERFILE:-Dockerfile}"
IMAGE_LABELS="${IMAGE_LABELS:-}"
SCAN_PLATFORM="${SCAN_PLATFORM:-linux/amd64}"
: "${TRIVY_IMAGE:?TRIVY_IMAGE is required}"
TRIVY_CACHE_DIR="${TRIVY_CACHE_DIR:-.trivy-cache}"
SCAN_RESULT_FILE="${SCAN_RESULT_FILE:-${RUNNER_TEMP:-/tmp}/scan-passed.txt}"

platform_suffix=${SCAN_PLATFORM//\//-}
LOCAL_IMAGE="${LOCAL_IMAGE:-local/sts-opentelemetry-collector:${GITHUB_SHA:-manual}-scan-${platform_suffix}}"

label_args=()
while IFS= read -r label; do
  if [[ -n "${label}" ]]; then
    label_args+=(--label "${label}")
  fi
done <<< "${IMAGE_LABELS}"

mkdir -p "${TRIVY_CACHE_DIR}" "$(dirname "${SCAN_RESULT_FILE}")"
rm -f "${SCAN_RESULT_FILE}"

echo "Building ${LOCAL_IMAGE} for ${SCAN_PLATFORM}"
build_args=(
  buildx
  build
  --platform "${SCAN_PLATFORM}"
  --load
  --provenance=false
  --sbom=false
  --tag "${LOCAL_IMAGE}"
  --file "${DOCKERFILE}"
)
if [[ "${#label_args[@]}" -gt 0 ]]; then
  build_args+=("${label_args[@]}")
fi
build_args+=(.)

docker "${build_args[@]}"

echo "Pulling scanner image ${TRIVY_IMAGE}"
docker pull "${TRIVY_IMAGE}"

scanner_args=(
  run
  --rm
  -v /var/run/docker.sock:/var/run/docker.sock
  -v "${PWD}/${TRIVY_CACHE_DIR}:/root/.cache/trivy"
)

# Mount the project's .trivyignore.yaml so accepted CVE exceptions
# (with expiration dates) apply to the vuln scan below. See the file
# itself for entry format and rationale.
ignorefile_args=()
if [[ -f "${PWD}/.trivyignore.yaml" ]]; then
  scanner_args+=(-v "${PWD}/.trivyignore.yaml:/scan/.trivyignore.yaml:ro")
  ignorefile_args=(--ignorefile /scan/.trivyignore.yaml)
fi

scanner_args+=(
  "${TRIVY_IMAGE}"
  image
  --no-progress
  --exit-code 1
)

echo "Scanning ${LOCAL_IMAGE} for secrets"
docker "${scanner_args[@]}" \
  --scanners secret \
  --severity UNKNOWN,LOW,MEDIUM,HIGH,CRITICAL \
  "${LOCAL_IMAGE}"

echo "Scanning ${LOCAL_IMAGE} for HIGH/CRITICAL vulnerabilities"
docker "${scanner_args[@]}" \
  --scanners vuln \
  --severity HIGH,CRITICAL \
  "${ignorefile_args[@]}" \
  "${LOCAL_IMAGE}"

{
  printf 'scan_platform=%s\n' "${SCAN_PLATFORM}"
  printf 'local_image=%s\n' "${LOCAL_IMAGE}"
} > "${SCAN_RESULT_FILE}"

echo "Required ${SCAN_PLATFORM} image passed scanning"
