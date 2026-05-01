#!/usr/bin/env bash

set -euo pipefail

: "${IMAGE_TAGS:?IMAGE_TAGS is required}"

DOCKERFILE="${DOCKERFILE:-Dockerfile}"
IMAGE_LABELS="${IMAGE_LABELS:-}"
PUBLISH_PLATFORMS="${PUBLISH_PLATFORMS:-linux/amd64,linux/arm64/v8}"
SCAN_RESULT_FILE="${SCAN_RESULT_FILE:-${RUNNER_TEMP:-/tmp}/scan-passed.txt}"

if [[ ! -s "${SCAN_RESULT_FILE}" ]]; then
  echo "Missing successful scan marker ${SCAN_RESULT_FILE}" >&2
  exit 1
fi

scan_platform=""
local_image=""
while IFS='=' read -r key value; do
  case "${key}" in
    scan_platform) scan_platform="${value}" ;;
    local_image) local_image="${value}" ;;
  esac
done < "${SCAN_RESULT_FILE}"

: "${scan_platform:?scan_platform missing from ${SCAN_RESULT_FILE}}"
: "${local_image:?local_image missing from ${SCAN_RESULT_FILE}}"

image_tags=()
while IFS= read -r tag; do
  if [[ -n "${tag}" ]]; then
    image_tags+=("${tag}")
  fi
done <<< "${IMAGE_TAGS}"

if [[ "${#image_tags[@]}" -eq 0 ]]; then
  echo "No image tags were provided" >&2
  exit 1
fi

label_args=()
while IFS= read -r label; do
  if [[ -n "${label}" ]]; then
    label_args+=(--label "${label}")
  fi
done <<< "${IMAGE_LABELS}"

normalize_platforms() {
  local raw_platforms=$1
  local platform
  local platforms=()

  IFS=',' read -r -a platforms <<< "${raw_platforms}"
  for platform in "${platforms[@]}"; do
    platform=${platform//[[:space:]]/}
    if [[ -n "${platform}" ]]; then
      printf '%s\n' "${platform}"
    fi
  done
}

platform_suffix() {
  local platform=$1
  printf '%s' "${platform//\//-}"
}

platform_list=()
while IFS= read -r platform; do
  platform_list+=("${platform}")
done < <(normalize_platforms "${PUBLISH_PLATFORMS}")

if [[ "${#platform_list[@]}" -eq 0 ]]; then
  echo "No publish platforms were provided" >&2
  exit 1
fi

scan_platform_requested=false
for platform in "${platform_list[@]}"; do
  if [[ "${platform}" == "${scan_platform}" ]]; then
    scan_platform_requested=true
  fi
done

if [[ "${scan_platform_requested}" != "true" ]]; then
  echo "Scan platform ${scan_platform} is not included in publish platforms: ${PUBLISH_PLATFORMS}" >&2
  exit 1
fi

image_refs=()
primary_tag="${image_tags[0]}"

for platform in "${platform_list[@]}"; do
  suffix=$(platform_suffix "${platform}")
  primary_platform_tag="${primary_tag}-${suffix}"

  if [[ "${platform}" == "${scan_platform}" ]]; then
    echo "Publishing already-scanned ${platform} image ${local_image}"
    for tag in "${image_tags[@]}"; do
      platform_tag="${tag}-${suffix}"
      docker tag "${local_image}" "${platform_tag}"
      docker push "${platform_tag}"
    done
  else
    tag_args=()
    for tag in "${image_tags[@]}"; do
      tag_args+=(--tag "${tag}-${suffix}")
    done

    echo "Publishing unscanned ${platform} image"
    docker buildx build \
      --platform "${platform}" \
      --push \
      --provenance=false \
      --sbom=false \
      "${tag_args[@]}" \
      --file "${DOCKERFILE}" \
      "${label_args[@]}" \
      .
  fi

  image_refs+=("${primary_platform_tag}")
done

manifest_tag_args=()
for tag in "${image_tags[@]}"; do
  manifest_tag_args+=(--tag "${tag}")
done

echo "Creating multi-platform manifest for ${image_tags[*]} from ${image_refs[*]}"
docker buildx imagetools create "${manifest_tag_args[@]}" "${image_refs[@]}"

digest=$(docker buildx imagetools inspect "${primary_tag}" | awk '/^Digest:/ { print $2; exit }')
if [[ -z "${digest}" ]]; then
  echo "Could not determine digest for ${primary_tag}" >&2
  exit 1
fi

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "digest=${digest}" >> "${GITHUB_OUTPUT}"
fi
echo "Published ${primary_tag}@${digest}"
