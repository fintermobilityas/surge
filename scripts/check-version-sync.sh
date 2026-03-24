#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/version-lib.sh"

workspace_version="$(workspace_base_version)"
surge_core_version="$(workspace_surge_core_version)"

if [[ -z "$workspace_version" || -z "$surge_core_version" ]]; then
  echo "Failed to parse version values from Cargo.toml." >&2
  exit 1
fi

failed=0

if [[ "$workspace_version" != "$surge_core_version" ]]; then
  cat <<EOF >&2
Version mismatch:
  Cargo.toml [workspace.package].version:          $workspace_version
  Cargo.toml [workspace.dependencies].surge-core:  $surge_core_version
Update Cargo.toml so the workspace package version and surge-core workspace dependency version match.
EOF
  failed=1
fi

# Check Cargo.lock is in sync
lock_file="$repo_root/Cargo.lock"
if [[ -f "$lock_file" ]]; then
  for crate in surge-core surge-cli surge-ffi surge-supervisor surge-installer surge-installer-ui; do
    lock_version=$(awk -v pkg="$crate" '
      /^\[\[package\]\]/ { in_pkg = 0 }
      $0 == "name = \"" pkg "\"" { in_pkg = 1; next }
      in_pkg && /^version = / { gsub(/"/, "", $3); print $3; exit }
    ' "$lock_file")
    if [[ -n "$lock_version" && "$lock_version" != "$workspace_version" ]]; then
      echo "Cargo.lock version mismatch: $crate is $lock_version, expected $workspace_version. Run 'cargo check' to regenerate." >&2
      failed=1
    fi
  done
fi

if [[ "$failed" -eq 1 ]]; then
  exit 1
fi

echo "Version sync check passed: $workspace_version"
