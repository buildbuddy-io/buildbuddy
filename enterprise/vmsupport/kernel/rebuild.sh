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

SHA256=$(sha256sum "$PWD/vmlinux" | awk '{print $1}')
NAME="vmlinux-${VERSION}-${SHA256}"
mv vmlinux "$NAME"

echo "---"
echo "Successfully built $PWD/$NAME"
echo "Uploading to https://storage.googleapis.com/buildbuddy-tools/binaries/linux/$NAME ..."

(
  set -x
  gsutil cp "$PWD/$NAME" "gs://buildbuddy-tools/binaries/linux/$NAME"
) || {
  echo "Upload failed; please upload manually."
}

echo ""
echo "deps.bzl update is required:"
echo ""
echo "    http_file("
echo "        name = \"org_kernel_git_linux_kernel-vmlinux\","
echo "        sha256 = \"$SHA256\","
echo "        urls = [\"https://storage.googleapis.com/buildbuddy-tools/binaries/linux/$NAME\"],"
echo "        executable = True,"
echo "    )"
