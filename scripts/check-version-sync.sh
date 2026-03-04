#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
gitversion_file="$repo_root/GitVersion.yml"
cargo_file="$repo_root/Cargo.toml"

next_version="$(
  awk -F': *' '/^next-version:/ {print $2; exit}' "$gitversion_file" \
    | tr -d '[:space:]"'
)"

workspace_version="$(
  awk '
    /^\[workspace\.package\]$/ { in_section = 1; next }
    /^\[/ { in_section = 0 }
    in_section && $1 == "version" {
      value = $3
      gsub(/"/, "", value)
      print value
      exit
    }
  ' "$cargo_file"
)"

surge_core_dep_version="$(
  awk '
    /^\[workspace\.dependencies\]$/ { in_section = 1; next }
    /^\[/ { in_section = 0 }
    in_section && $1 == "surge-core" {
      if (match($0, /version[[:space:]]*=[[:space:]]*"[^"]+"/)) {
        value = substr($0, RSTART, RLENGTH)
        split(value, parts, "\"")
        print parts[2]
      }
      exit
    }
  ' "$cargo_file"
)"

if [[ -z "$next_version" || -z "$workspace_version" || -z "$surge_core_dep_version" ]]; then
  echo "Failed to parse version values from GitVersion.yml/Cargo.toml." >&2
  exit 1
fi

if [[ "$workspace_version" != "$next_version" ]]; then
  cat <<EOF >&2
Version mismatch:
  GitVersion.yml next-version:     $next_version
  Cargo.toml workspace version:    $workspace_version
Update [workspace.package].version in Cargo.toml to match GitVersion.yml.
EOF
  exit 1
fi

if [[ "$surge_core_dep_version" != "$next_version" ]]; then
  cat <<EOF >&2
Version mismatch:
  GitVersion.yml next-version:                   $next_version
  Cargo.toml workspace surge-core dep version:   $surge_core_dep_version
Update [workspace.dependencies].surge-core version to match GitVersion.yml.
EOF
  exit 1
fi

echo "Version sync check passed: $next_version"
