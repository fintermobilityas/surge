#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Build and stage a Surge publishing toolchain into a single directory.

Usage:
  stage-toolchain-artifact.sh --output <dir> [--with-gui]

This script is intended for CI bootstrap jobs that build Surge once per host
architecture, then upload the staged directory as a workflow artifact for later
publish jobs.
EOF
}

output_dir=""
with_gui=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --output)
      output_dir="${2:-}"
      shift 2
      ;;
    --with-gui)
      with_gui=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown argument: %s\n\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [ -z "$output_dir" ]; then
  printf -- '--output is required.\n\n' >&2
  usage >&2
  exit 1
fi

if [ ! -f Cargo.toml ] || [ ! -d crates/surge-core ] || [ ! -d crates/surge-cli ]; then
  printf 'Run this script from the Surge repository root.\n' >&2
  exit 1
fi

packages=(-p surge-cli -p surge-supervisor -p surge-installer -p surge-ffi)
if [ "$with_gui" -eq 1 ]; then
  packages+=(-p surge-installer-ui)
fi

cargo build --release "${packages[@]}"

case "$(uname -s | tr '[:upper:]' '[:lower:]')" in
  linux)
    native_runtime="libsurge.so"
    binaries=(surge surge-supervisor surge-installer)
    [ "$with_gui" -eq 1 ] && binaries+=(surge-installer-ui)
    ;;
  darwin)
    native_runtime="libsurge.dylib"
    binaries=(surge surge-supervisor surge-installer)
    [ "$with_gui" -eq 1 ] && binaries+=(surge-installer-ui)
    ;;
  msys*|mingw*|cygwin*)
    native_runtime="surge.dll"
    binaries=(surge.exe surge-supervisor.exe surge-installer.exe)
    [ "$with_gui" -eq 1 ] && binaries+=(surge-installer-ui.exe)
    ;;
  *)
    printf 'Unsupported host OS.\n' >&2
    exit 1
    ;;
esac

rm -rf "$output_dir"
mkdir -p "$output_dir"

for binary in "${binaries[@]}"; do
  cp "target/release/${binary}" "$output_dir/"
done
cp "target/release/${native_runtime}" "$output_dir/"

printf 'Staged Surge toolchain in %s\n' "$output_dir"
