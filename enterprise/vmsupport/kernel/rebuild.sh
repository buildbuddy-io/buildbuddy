#!/bin/bash
set -e

# usage: VERSION=vX.Y enterprise/vmsupport/kernel/rebuild.sh

: "${VERSION:=v5.4}"
: "${WORKDIR:=/tmp/linux}"
: "${UPLOAD:=1}"

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

# Set up a bazel workspace compatible with --override_repository
rm -rf ./bazel_override
mkdir bazel_override
touch bazel_override/WORKSPACE
mkdir bazel_override/file
ln "$NAME" bazel_override/file/downloaded
echo 'exports_files(["downloaded"])' >bazel_override/file/BUILD

echo "---"
echo "Successfully built $PWD/$NAME"

echo "Bazel repository override:"
echo "  --override_repository=org_kernel_git_linux_kernel-vmlinux=$PWD/bazel_override"

UPLOAD_COMMAND=(gsutil cp "$PWD/$NAME" "gs://buildbuddy-tools/binaries/linux/$NAME")
if ((UPLOAD)); then
  echo "Uploading to https://storage.googleapis.com/buildbuddy-tools/binaries/linux/$NAME ..."
  (
    echo "Running" "${UPLOAD_COMMAND[@]}"
    "${UPLOAD_COMMAND[@]}"

    echo ""
    echo "deps.bzl update is required:"
    echo ""
    echo "    http_file("
    echo "        name = \"org_kernel_git_linux_kernel-vmlinux\","
    echo "        sha256 = \"$SHA256\","
    echo "        urls = [\"https://storage.googleapis.com/buildbuddy-tools/binaries/linux/$NAME\"],"
    echo "        executable = True,"
    echo "    )"
  ) || {
    echo "Upload failed; please upload manually, and update deps.bzl"
  }
else
  echo "To upload to gcs, run:" "${UPLOAD_COMMAND[@]}"
fi
