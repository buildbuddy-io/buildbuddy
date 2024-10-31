#!/bin/bash
set -e

# usage: VERSION=vX.Y enterprise/vmsupport/kernel/rebuild.sh

: "${VERSION:=v5.4}"
: "${WORKDIR:=/tmp/linux}"

cd "$(dirname "$0")"
SCRIPT_DIR="$PWD"
ARCH=$(uname -m)
KERNEL_CONFIG_PATH="$PWD/microvm-kernel-$ARCH.config"

mkdir -p "$WORKDIR" && cd "$WORKDIR"
echo "Working directory: $PWD"
if ! [[ -e .git ]]; then
  git clone --depth 1 --branch "$VERSION" git://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git .
fi
git fetch origin "$VERSION" --depth=1 && git checkout "$VERSION"

# Fix for the error
#   (.text+0x0): multiple definition of `yylloc'; dtc-lexer.lex.o (symbol from plugin):(.text+0x0): first defined here
# TODO: this can be removed in kernel v5.6+
git apply "$SCRIPT_DIR/dtc-lexer.patch" || true

cp "$KERNEL_CONFIG_PATH" .config
make olddefconfig

MAKE_TARGET=vmlinux
OUTPUT_PATH=./vmlinux
if [[ $ARCH == aarch64 ]]; then
  MAKE_TARGET=Image
  OUTPUT_PATH=./arch/arm64/boot/Image
fi
make -j 16 "$MAKE_TARGET"

SHA256=$(sha256sum "$OUTPUT_PATH" | awk '{print $1}')
NAME="vmlinux-${ARCH}-${VERSION}-${SHA256}"
mv "${OUTPUT_PATH}" "$NAME"

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
