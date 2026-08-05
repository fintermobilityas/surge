#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Publish the Surge crates for an exact version, safely supporting workflow reruns.

Usage:
  publish-cargo-release.sh <version>
EOF
}

if [ "$#" -ne 1 ] || [[ ! "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-(alpha|beta)\.[0-9]+)?$ ]]; then
  usage >&2
  exit 1
fi

version="$1"
api_body="$(mktemp)"
trap 'rm -f "${api_body}"' EXIT

crate_visibility() {
  local crate="$1"
  local status

  if ! status="$(curl \
    --silent \
    --show-error \
    --output "${api_body}" \
    --write-out '%{http_code}' \
    --user-agent "surge-release-workflow/1.0 (${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-fintermobilityas/surge})" \
    "https://crates.io/api/v1/crates/${crate}/${version}")"; then
    printf 'Failed to query crates.io for %s %s\n' "${crate}" "${version}" >&2
    exit 1
  fi

  case "${status}" in
    200)
      printf 'present\n'
      ;;
    404)
      printf 'absent\n'
      ;;
    *)
      printf 'Unexpected crates.io response for %s %s: HTTP %s\n' \
        "${crate}" "${version}" "${status}" >&2
      sed -n '1,20p' "${api_body}" >&2
      exit 1
      ;;
  esac
}

publish_with_retries() {
  local crate="$1"
  local max_attempts="$2"
  local attempt
  local visibility

  visibility="$(crate_visibility "${crate}")"
  if [ "${visibility}" = "present" ]; then
    printf '%s %s is already visible on crates.io; skipping publish.\n' \
      "${crate}" "${version}"
    return
  fi

  for attempt in $(seq 1 "${max_attempts}"); do
    printf 'Publishing %s %s to crates.io (attempt %s/%s).\n' \
      "${crate}" "${version}" "${attempt}" "${max_attempts}"
    if cargo publish -p "${crate}" --allow-dirty; then
      return
    fi

    visibility="$(crate_visibility "${crate}")"
    if [ "${visibility}" = "present" ]; then
      printf '%s %s became visible after cargo publish returned an error; continuing.\n' \
        "${crate}" "${version}"
      return
    fi

    if [ "${attempt}" -lt "${max_attempts}" ]; then
      printf 'Publish failed while %s %s is still absent; waiting for registry/index propagation.\n' \
        "${crate}" "${version}"
      sleep $((attempt * 5))
    fi
  done

  printf 'cargo publish failed after %s attempts and %s %s is still absent from crates.io.\n' \
    "${max_attempts}" "${crate}" "${version}" >&2
  return 1
}

wait_until_visible() {
  local crate="$1"
  local attempt
  local visibility

  for attempt in $(seq 1 30); do
    visibility="$(crate_visibility "${crate}")"
    if [ "${visibility}" = "present" ]; then
      printf '%s %s is visible on crates.io.\n' "${crate}" "${version}"
      return
    fi

    if [ "${attempt}" -lt 30 ]; then
      printf 'Waiting for %s %s to become visible on crates.io (%s/30).\n' \
        "${crate}" "${version}" "${attempt}"
      sleep 10
    fi
  done

  printf '%s %s did not become visible on crates.io within five minutes.\n' \
    "${crate}" "${version}" >&2
  exit 1
}

publish_with_retries surge-core 3
wait_until_visible surge-core
publish_with_retries surge-cli 10
wait_until_visible surge-cli
