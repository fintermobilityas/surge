#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Validate the macOS runner, Rust host, and Mach-O architecture of release files.

Usage:
  check-macos-architecture.sh --arch <x86_64|arm64> [--runner-only] [<file> ...]
EOF
}

expected_arch=""
runner_only="false"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --arch)
      expected_arch="${2:-}"
      shift 2
      ;;
    --runner-only)
      runner_only="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      printf 'Unknown argument: %s\n\n' "$1" >&2
      usage >&2
      exit 1
      ;;
    *)
      break
      ;;
  esac
done

case "${expected_arch}" in
  x86_64)
    expected_rust_host="x86_64-apple-darwin"
    ;;
  arm64)
    expected_rust_host="aarch64-apple-darwin"
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac

if [ "${runner_only}" = "false" ] && [ "$#" -eq 0 ]; then
  usage >&2
  exit 1
fi

runner_arch="$(uname -m)"
if [ "${runner_arch}" != "${expected_arch}" ]; then
  printf 'Unexpected macOS runner architecture: found %s, expected %s\n' \
    "${runner_arch}" "${expected_arch}" >&2
  exit 1
fi

rust_host="$(rustc -vV | sed -n 's/^host: //p')"
if [ "${rust_host}" != "${expected_rust_host}" ]; then
  printf 'Unexpected Rust host: found %s, expected %s\n' \
    "${rust_host}" "${expected_rust_host}" >&2
  exit 1
fi

printf 'macOS runner and Rust host OK: %s / %s\n' "${runner_arch}" "${rust_host}"

if [ "${runner_only}" = "true" ]; then
  if [ "$#" -ne 0 ]; then
    printf '%s\n' '--runner-only does not accept file arguments.' >&2
    exit 1
  fi
  exit 0
fi

for path in "$@"; do
  if [ ! -f "${path}" ]; then
    printf 'Missing Mach-O file: %s\n' "${path}" >&2
    exit 1
  fi

  if ! actual_archs="$(lipo -archs "${path}")"; then
    printf 'Could not read Mach-O architectures from %s\n' "${path}" >&2
    exit 1
  fi

  if [ "${actual_archs}" != "${expected_arch}" ]; then
    printf 'Mach-O architecture mismatch in %s: found %s, expected only %s\n' \
      "${path}" "${actual_archs}" "${expected_arch}" >&2
    exit 1
  fi

  printf 'Mach-O architecture OK in %s: %s\n' "${path}" "${expected_arch}"
done
