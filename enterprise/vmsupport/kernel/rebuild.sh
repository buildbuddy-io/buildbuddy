#!/bin/bash
set -e

# usage: VERSION=vX.Y enterprise/vmsupport/kernel/rebuild.sh

: "${VERSION:=v5.4}"
: "${WORKDIR:=/tmp/linux}"

cd "$(dirname "$0")"
KERNEL_CONFIG_PATH="$PWD/microvm-kernel-x86_64.config"

mkdir -p "$WORKDIR" && cd "$WORKDIR"
echo "Working directory: $PWD"
if ! [[ -e .git ]]; then
  git clone --depth 1 --branch "$VERSION" git://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git .
fi
git fetch origin "$VERSION" --depth=1 && git checkout "$VERSION"
cp "$KERNEL_CONFIG_PATH" .config
make olddefconfig
make -j 16 vmlinux
cp vmlinux "vmlinux-$VERSION"

echo "---"
echo "Successfully built $PWD/vmlinux-$VERSION"
echo "Upload this file to https://storage.googleapis.com/buildbuddy-tools/binaries/linux/vmlinux-$VERSION"
echo "Then update deps.bzl and any targets which depend on the old vmlinux version."
echo "sha256: $(sha256sum "$PWD/vmlinux-$VERSION" | awk '{print $1}')"
