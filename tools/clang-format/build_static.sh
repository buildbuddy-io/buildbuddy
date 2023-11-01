#!/usr/bin/env bash
set -euo pipefail

# Fetches the clang-tools repo and builds a static build of clang-format from source.

: "${OUTDIR:=/tmp/clang-format}"

SHORT_VERSION="14"
LONG_VERSION="14.0.0"
RELEASE=llvm-project-14.0.0.src

mkdir -p "$OUTDIR"
cd "$OUTDIR"

COMMON_CMAKE_ARGS=(
  -DBUILD_SHARED_LIBS=OFF
  -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra"
)
POSIX_CMAKE_ARGS=(
  -DCMAKE_BUILD_TYPE=MinSizeRel
  -DCMAKE_CXX_COMPILER=g++-10
  -DCMAKE_C_COMPILER=gcc-10
)

if [[ "$OSTYPE" == linux-* ]]; then
  OS=linux
  CMAKE_ARGS=(
    -DLLVM_BUILD_STATIC=ON
    -DCMAKE_CXX_FLAGS="-s -flto"
    "${POSIX_CMAKE_ARGS[@]}"
  )
else
  OS=darwin
  CMAKE_ARGS=(
    -DCMAKE_CXX_FLAGS="-static-libgcc -static-libstdc++ -flto"
    -DCMAKE_OSX_DEPLOYMENT_TARGET=10.15
    "${POSIX_CMAKE_ARGS[@]}"
  )

  # TODO: finish making this work on macOS.
  # Apparently some ugly gcc patching is needed...
  # https://github.com/buildbuddy-io/clang-tools-static-binaries/blob/57bde8bee66ea74291de4cb80a7dfeecdf682f6c/.github/workflows/clang-tools-amd64.yml#L158
fi

ARCH=$(uname -m)

mkdir -p build
cd build

if ! [[ -e "$RELEASE.tar.xz" ]]; then
  rm -rf ./*
  curl -LO "https://github.com/llvm/llvm-project/releases/download/llvmorg-$LONG_VERSION/$RELEASE.tar.xz"
  tar xf $RELEASE.tar.xz
fi
cmake -S "$RELEASE"/llvm -B "$RELEASE"/build "${COMMON_CMAKE_ARGS[@]}" "${CMAKE_ARGS[@]}"
cmake --build $RELEASE/build -j"$(nproc)" --target clang-format

NAME="clang-format-${SHORT_VERSION}_${OS}-${ARCH}"
mv "$RELEASE"/build/bin/clang-format "$NAME"
echo "Wrote $PWD/$NAME"
echo "SHA256: $(sha256sum <"$NAME" | awk '{print $1}')"
