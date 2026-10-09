#!/usr/bin/env bash
set -euo pipefail

# Builds static crun binaries for linux/amd64 and linux/arm64 with
# buildpatches/crun_no_enable_controllers.patch applied, then prints their
# sha256 digests. To release a new build, upload the binaries to
# gs://buildbuddy-tools/binaries/crun/ under a new name and update the crun
# http_file rules in deps.bzl.
#
# Building both platforms at once needs a buildx builder that supports
# multi-platform builds, such as one created with
# "docker buildx create --driver=docker-container". Select it with
# BUILDX_BUILDER. The arm64 build runs under emulation unless the builder has
# a native arm64 node, so the host needs qemu binfmt handlers for arm64.

: "${VERSION:=1.28}"
: "${SOURCE_SHA256:=eb8fe73ffe44d868b14bb94fa6c295bd57e8bf023de43b61579da826c07cc406}"
: "${OUT_DIR:=/tmp/crun-build}"

cd "$(dirname "$0")/.."

docker buildx build \
  --platform=linux/amd64,linux/arm64 \
  --build-arg=VERSION="$VERSION" \
  --build-arg=SOURCE_SHA256="$SOURCE_SHA256" \
  --output=type=local,dest="$OUT_DIR" \
  --file=- \
  buildpatches <<'EOF'
FROM mirror.gcr.io/library/alpine@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1 AS build
RUN apk add --no-cache \
    argp-standalone bash build-base json-c-dev libcap-dev libcap-static \
    libseccomp-dev libseccomp-static linux-headers pkgconf python3
ARG VERSION
ARG SOURCE_SHA256
WORKDIR /src
RUN wget -O crun.tar.gz "https://github.com/containers/crun/releases/download/${VERSION}/crun-${VERSION}.tar.gz" && \
    echo "${SOURCE_SHA256}  crun.tar.gz" | sha256sum -c - && \
    tar -xzf crun.tar.gz --strip-components=1
COPY crun_no_enable_controllers.patch .
RUN patch -p1 < crun_no_enable_controllers.patch && \
    cp .tarball-git-version.h git-version.h && \
    ./configure --disable-systemd --disable-shared --disable-dl \
        --disable-criu --disable-maintainer-mode --enable-embedded-blake3 \
        CFLAGS=-O2 LDFLAGS=-static && \
    make -j"$(nproc)" crun CRUN_LDFLAGS=-all-static && \
    if readelf -lWd crun | grep -Eq 'INTERP|NEEDED'; then \
        echo "crun is not statically linked" >&2; exit 1; \
    fi

FROM scratch
COPY --from=build /src/crun /crun
EOF

sha256sum "$OUT_DIR"/linux_amd64/crun "$OUT_DIR"/linux_arm64/crun
