FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    gcc-13 \
    g++-13 \
    ninja-build \
    pkg-config \
    git \
    curl \
    zip \
    unzip \
    tar \
    && rm -rf /var/lib/apt/lists/*

ENV CC=gcc-13
ENV CXX=g++-13

RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics

ENV VCPKG_ROOT=/opt/vcpkg
ENV PATH="${VCPKG_ROOT}:${PATH}"

WORKDIR /src
COPY vcpkg.json vcpkg-configuration.json CMakeLists.txt CMakePresets.json ./
COPY cmake/ cmake/
COPY include/ include/
COPY src/ src/
COPY vendor/ vendor/
COPY tests/ tests/

RUN cmake --preset linux-x64-release \
    && cmake --build --preset linux-x64-release

FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4 \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/linux-x64-release/surge /usr/local/bin/surge
COPY --from=builder /src/build/linux-x64-release/surge_supervisor /usr/local/bin/surge_supervisor
COPY --from=builder /src/build/linux-x64-release/libsurge_core.so /usr/local/lib/

RUN ldconfig

ENTRYPOINT ["surge"]
