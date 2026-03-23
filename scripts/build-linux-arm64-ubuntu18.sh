#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Build and stage the Linux ARM64 Surge toolchain inside an Ubuntu 18.04 container.

Usage:
  build-linux-arm64-ubuntu18.sh --output <dir> [--with-gui]

The output directory must live inside the Surge repository checkout so the
container can write staged artifacts back to the host workspace.
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

if ! command -v docker >/dev/null 2>&1; then
  printf 'Docker is required to build the Ubuntu 18.04 ARM64 toolchain.\n' >&2
  exit 1
fi

repo_root="$(pwd -P)"
output_abs="$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "${output_dir}")"

case "${output_abs}" in
  "${repo_root}"/*) ;;
  *)
    printf 'Output directory must be inside the repository: %s\n' "${output_abs}" >&2
    exit 1
    ;;
esac

output_rel="${output_abs#${repo_root}/}"
mkdir -p "${output_abs}"

docker run --rm --platform linux/arm64 \
  -e DEBIAN_FRONTEND=noninteractive \
  -e CARGO_HOME=/work/.cargo-bionic \
  -e RUSTUP_HOME=/work/.rustup-bionic \
  -e CARGO_TARGET_DIR=/work/target-bionic \
  -e OUTPUT_DIR="/work/${output_rel}" \
  -e WITH_GUI="${with_gui}" \
  -e HOST_UID="$(id -u)" \
  -e HOST_GID="$(id -g)" \
  -v "${repo_root}:/work" \
  -w /work \
  ubuntu:18.04 \
  bash -lc '
    set -euo pipefail
    apt-get update
    apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      clang \
      curl \
      file \
      git \
      libasound2-dev \
      libegl1-mesa-dev \
      libgl1-mesa-dev \
      libssl-dev \
      libudev-dev \
      libwayland-dev \
      libx11-dev \
      libx11-xcb-dev \
      libxcb-render0-dev \
      libxcb-shape0-dev \
      libxcb-xfixes0-dev \
      libxcursor-dev \
      libxi-dev \
      libxinerama-dev \
      libxkbcommon-dev \
      libxrandr-dev \
      libxxf86vm-dev \
      pkg-config \
      python3

    export PATH="${CARGO_HOME}/bin:${PATH}"
    if [ ! -x "${CARGO_HOME}/bin/cargo" ]; then
      curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
    fi

    stage_args=(--output "${OUTPUT_DIR}")
    if [ "${WITH_GUI}" = "1" ]; then
      stage_args+=(--with-gui)
    fi

    ./scripts/stage-toolchain-artifact.sh "${stage_args[@]}"

    chown -R "${HOST_UID}:${HOST_GID}" \
      /work/.cargo-bionic \
      /work/.rustup-bionic \
      /work/target-bionic \
      "${OUTPUT_DIR}"
  '
