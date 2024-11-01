#!/bin/bash
set -e

# usage: VERSION=vX.Y enterprise/vmsupport/kernel/rebuild.sh

: "${VERSION:=v5.10}"
: "${WORKDIR:=/tmp/linux}"

cd "$(dirname "$0")"
ARCH=$(uname -m)
KERNEL_CONFIG_PATH="$PWD/microvm-kernel-$ARCH.config"

mkdir -p "$WORKDIR" && cd "$WORKDIR"
echo "Working directory: $PWD"
if ! [[ -e .git ]]; then
  git clone --depth 1 --branch "$VERSION" git://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git .
fi
if [[ "${CLEAN:-}" == 1 ]]; then
  git clean -fdx
fi
git fetch origin "$VERSION" --depth=1
git checkout FETCH_HEAD --

cp "$KERNEL_CONFIG_PATH" .config
make olddefconfig

MAKE_TARGET=vmlinux
OUTPUT_PATH=./vmlinux
if [[ "$ARCH" == aarch64 ]]; then
  MAKE_TARGET=Image
  OUTPUT_PATH=./arch/arm64/boot/Image
fi
make -j 16 "$MAKE_TARGET"

SHA256=$(sha256sum "$OUTPUT_PATH" | awk '{print $1}')
NAME="vmlinux-${ARCH}-${VERSION}-${SHA256}"
rm -f "$NAME" || true
ln "${OUTPUT_PATH}" "$NAME"

echo "---"
echo "Successfully built $PWD/$NAME"
echo "Uploading to https://storage.googleapis.com/buildbuddy-tools/binaries/linux/$NAME ..."

(
  set -x
  gsutil cp "$PWD/$NAME" "gs://buildbuddy-tools/binaries/linux/$NAME"
) || {
  echo "Upload failed; please upload manually."
}

ARCH_SUFFIX=""
if [[ "$ARCH" == aarch64 ]]; then
  ARCH_SUFFIX="-arm64"
fi

echo ""
echo "deps.bzl update is required:"
echo ""
echo "    http_file("
echo "        name = \"org_kernel_git_linux_kernel-vmlinux${ARCH_SUFFIX}\","
echo "        sha256 = \"$SHA256\","
echo "        urls = [\"https://storage.googleapis.com/buildbuddy-tools/binaries/linux/$NAME\"],"
echo "        executable = True,"
echo "    )"
