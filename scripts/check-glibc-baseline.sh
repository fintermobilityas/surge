#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Validate that ELF binaries do not require a newer glibc than the requested maximum.

Usage:
  check-glibc-baseline.sh --max <version> <file> [<file> ...]
EOF
}

max_version=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --max)
      max_version="${2:-}"
      shift 2
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

if [ -z "${max_version}" ] || [ "$#" -eq 0 ]; then
  usage >&2
  exit 1
fi

status=0

for path in "$@"; do
  if [ ! -f "${path}" ]; then
    printf 'Missing file: %s\n' "${path}" >&2
    status=1
    continue
  fi

  highest="$(strings "${path}" | grep -oE 'GLIBC_[0-9]+\.[0-9]+' | sed 's/^GLIBC_//' | sort -V | tail -n 1 || true)"
  if [ -z "${highest}" ]; then
    printf 'No glibc symbols found in %s\n' "${path}"
    continue
  fi

  if [ "$(printf '%s\n%s\n' "${highest}" "${max_version}" | sort -V | tail -n 1)" != "${max_version}" ]; then
    printf 'glibc requirement too new in %s: found %s, max allowed %s\n' "${path}" "${highest}" "${max_version}" >&2
    status=1
  else
    printf 'glibc OK in %s: %s <= %s\n' "${path}" "${highest}" "${max_version}"
  fi
done

exit "${status}"
