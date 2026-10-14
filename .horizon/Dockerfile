# syntax=docker/dockerfile:1.7
FROM ghcr.io/peters/horizon-worker-base@sha256:9f233978dedd3d99214990ff8346e9539d4c127d5e827c74488e21346823b8f2

USER root
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Native compiler tools supplied by the hosted Linux runner, plus egui runtime.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential gcc-13 g++-13 clang-18 cmake pkg-config \
      curl ca-certificates jq gawk diffutils file python3 zstd \
      zlib1g-dev libssl-dev libicu74 libunwind8 liblttng-ust1t64 \
      libasound2-dev libudev-dev libwayland-dev \
      libegl1-mesa-dev libgl1-mesa-dev libgl1-mesa-dri \
      libx11-dev libx11-xcb-dev libxcb-render0-dev libxcb-shape0-dev \
      libxcb-xfixes0-dev libxcursor-dev libxi-dev libxinerama-dev \
      libxkbcommon-dev libxkbcommon-x11-0 libxrandr-dev libxxf86vm-dev \
      fonts-dejavu-core fonts-liberation

# rust-toolchain.toml and the MSRV job both require 1.95.0.
ENV RUSTUP_HOME=/opt/surge/rustup
RUN curl -fsSL https://static.rust-lang.org/rustup/archive/1.29.1/x86_64-unknown-linux-gnu/rustup-init -o /tmp/rustup-init \
    && echo 'dda7234360b7f578ca8b0ddcb80145646fa61a67c1720a5abc7051b35c9fcb71  /tmp/rustup-init' | sha256sum -c - \
    && chmod +x /tmp/rustup-init \
    && CARGO_HOME=/opt/surge/cargo /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain 1.95.0 --component rustfmt,clippy \
    && chown -R 10001:10001 /opt/surge/rustup \
    && rm /tmp/rustup-init

# Exact actions/setup-dotnet pin from both .NET CI jobs. SHA512: Microsoft metadata.
RUN mkdir -p /opt/surge/dotnet \
    && curl -fsSL https://builds.dotnet.microsoft.com/dotnet/Sdk/11.0.100-rc.2.26504.105/dotnet-sdk-11.0.100-rc.2.26504.105-linux-x64.tar.gz -o /tmp/dotnet.tar.gz \
    && echo '54e9463339337cef68f455e97dadac0a9e1b49416a3b15504b577144b24ae4b4eed4e7ba3bac507650b61edf704e977b4627d3804f4c2688cc5bd69b4f2eecdd  /tmp/dotnet.tar.gz' | sha512sum -c - \
    && tar -xzf /tmp/dotnet.tar.gz -C /opt/surge/dotnet \
    && rm /tmp/dotnet.tar.gz

# Latest localci release at setup time; verify both the published sums and pin.
RUN mkdir /tmp/localci \
    && cd /tmp/localci \
    && curl -fsSLO https://github.com/peters/localci/releases/download/v0.1.0/localci-x86_64-unknown-linux-musl.tar.gz \
    && curl -fsSLO https://github.com/peters/localci/releases/download/v0.1.0/SHA256SUMS \
    && awk '$2 == "localci-x86_64-unknown-linux-musl.tar.gz"' SHA256SUMS > selected.sha256 \
    && test -s selected.sha256 \
    && sha256sum -c selected.sha256 \
    && echo '215096651a2af9b1a8d64825d6453b7e090049078ac30525074e9e1f43fdc893  localci-x86_64-unknown-linux-musl.tar.gz' | sha256sum -c - \
    && tar -xzf localci-x86_64-unknown-linux-musl.tar.gz \
    && install -m 0755 localci /usr/local/bin/localci \
    && rm -rf /tmp/localci

# Cargo's default registry cache stays under the persistent session HOME.
RUN mkdir -p /etc/horizon-worker/session-env.d \
    && printf '%s\n' \
      'PATH_PREPEND=/opt/surge/cargo/bin' \
      'PATH_PREPEND=/opt/surge/dotnet' \
      'PATH_PREPEND=/usr/local/bin' \
      'RUSTUP_HOME=/opt/surge/rustup' \
      'DOTNET_ROOT=/opt/surge/dotnet' \
      'DOTNET_CLI_TELEMETRY_OPTOUT=1' \
      'DOTNET_NOLOGO=1' \
      'CARGO_BUILD_JOBS=4' \
      'RUST_TEST_THREADS=1' \
      'TMPDIR=/workspace/home' \
      'CC=gcc-13' \
      'CXX=g++-13' \
      > /etc/horizon-worker/session-env.d/20-surge.env

COPY preview.sh /usr/local/bin/surge-worker-preview
RUN chmod 0755 /usr/local/bin/surge-worker-preview

# Proof containers override the worker's init entrypoint. Reap detached test
# processes even when localci runs below a shell rather than the normal init.
RUN mkdir -p /usr/local/libexec \
    && mv /usr/local/bin/localci /usr/local/libexec/localci \
    && printf '%s\n' '#!/bin/sh' \
      'mkdir -p "${TMPDIR:-/tmp}" || exit 1' \
      'exec /usr/bin/tini -s -- /usr/local/libexec/localci "$@"' \
      > /usr/local/bin/localci \
    && chmod 0755 /usr/local/bin/localci
