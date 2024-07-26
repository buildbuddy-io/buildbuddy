#!/usr/bin/env bash
set -euo pipefail

: "${VERSION:=v1.36.1}"
: "${REPO_DIR:=/tmp/busybox}"

if ! [[ -e "$REPO_DIR" ]]; then
  mkdir -p "$REPO_DIR"
  (
    cd "$REPO_DIR"
    git init
    git remote add origin https://git.busybox.net/busybox
  )
fi
cd "$REPO_DIR"

# For version vX.Y.Z the git tag is X_Y_Z
TAG="$VERSION"
TAG="${TAG//v/}"
TAG="${TAG//./_}"

git clean -fdx
git fetch origin "$TAG" --depth=1
git checkout "$TAG"

make defconfig
sed -i 's/# CONFIG_STATIC is not set/CONFIG_STATIC=y/' .config
make

sha256sum "$REPO_DIR/busybox"
file "$REPO_DIR/busybox"
