#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Synchronize the vendored C sources used by surge-core from vendor/bsdiff.

Usage:
  ./scripts/sync-surge-core-vendor.sh [--check]

Without --check, this rewrites crates/surge-core/vendor to the expected
publishable snapshot. With --check, it exits non-zero if the current snapshot
does not match vendor/bsdiff.
EOF
}

mode="write"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --check)
      mode="check"
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

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source_root="$repo_root/vendor/bsdiff"
dest_root="$repo_root/crates/surge-core/vendor"
tmp_root="$(mktemp -d)"
snapshot_root="$tmp_root/vendor"

cleanup() {
  rm -rf "$tmp_root"
}
trap cleanup EXIT

if [ ! -d "$source_root/source" ] || [ ! -d "$source_root/include" ] || [ ! -d "$source_root/3rdparty" ]; then
  printf 'vendor/bsdiff is missing. Run git submodule update --init --recursive first.\n' >&2
  exit 1
fi

mkdir -p "$snapshot_root/3rdparty/libdivsufsort"

copy_file() {
  local relative_path="$1"

  mkdir -p "$(dirname -- "$snapshot_root/$relative_path")"
  cp "$source_root/$relative_path" "$snapshot_root/$relative_path"
}

copy_file "include/bsdiff.h"
copy_file "source/bsdiff.c"
copy_file "source/bsdiff_private.h"
copy_file "source/bspatch.c"
copy_file "source/compressor_bz2.c"
copy_file "source/decompressor_bz2.c"
copy_file "source/misc.c"
copy_file "source/patch_packer_bz2.c"
copy_file "source/stream_file.c"
copy_file "source/stream_memory.c"
copy_file "source/stream_sub.c"
copy_file "3rdparty/bzip2/blocksort.c"
copy_file "3rdparty/bzip2/bzlib.c"
copy_file "3rdparty/bzip2/bzlib.h"
copy_file "3rdparty/bzip2/bzlib_private.h"
copy_file "3rdparty/bzip2/compress.c"
copy_file "3rdparty/bzip2/crctable.c"
copy_file "3rdparty/bzip2/decompress.c"
copy_file "3rdparty/bzip2/huffman.c"
copy_file "3rdparty/bzip2/randtable.c"
copy_file "3rdparty/libdivsufsort/include/divsufsort.h.cmake"
copy_file "3rdparty/libdivsufsort/include/divsufsort_private.h"
copy_file "3rdparty/libdivsufsort/lib/divsufsort.c"
copy_file "3rdparty/libdivsufsort/lib/sssort.c"
copy_file "3rdparty/libdivsufsort/lib/trsort.c"
copy_file "3rdparty/libdivsufsort/lib/utils.c"

if [ "$mode" = "check" ]; then
  if ! diff -ruN "$snapshot_root" "$dest_root"; then
    printf '\nsurge-core vendor snapshot is stale. Run ./scripts/sync-surge-core-vendor.sh and commit the result.\n' >&2
    exit 1
  fi
  printf 'surge-core vendor snapshot is up to date.\n'
  exit 0
fi

rm -rf "$dest_root"
mkdir -p "$(dirname -- "$dest_root")"
cp -R "$snapshot_root" "$dest_root"
printf 'Synchronized %s from %s\n' "$dest_root" "$source_root"
