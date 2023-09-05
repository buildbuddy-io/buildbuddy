#!/usr/bin/env bash

#!/bin/bash
set -euo pipefail

: "${KERNEL_CLONE_DIR:=../linux}"
: "${KERNEL_REPO:=https://github.com/buildbuddy-io/linux}"
: "${KERNEL_BRANCH:=v5.15-debug}"

if ! [[ -e "$KERNEL_CLONE_DIR" ]]; then
  git clone "$KERNEL_REPO" "$KERNEL_CLONE_DIR"
  ( cd "$KERNEL_CLONE_DIR" && git checkout v5.15-debug )
fi

BB_DIR="$PWD"

cd "$KERNEL_CLONE_DIR"

cp "$BB_DIR"/enterprise/vmsupport/kernel/microvm-kernel-x86_64.config .config
make olddefconfig
make -j 16 vmlinux

write_file() {
  if [[ ! -e "$1" ]] || [[ "$(cat "$1" 2>/dev/null || true)" != "$2" ]]; then
    printf '%s' "$2" > "$1"
  fi
}

mkdir -p /tmp/vmlinux/file
write_file /tmp/vmlinux/WORKSPACE ''
write_file /tmp/vmlinux/file/BUILD 'exports_files(["downloaded"])'
ln -f vmlinux /tmp/vmlinux/file/downloaded

echo "==="
echo "Done!"
echo "Run bazel with:"
echo "    --override_repository=org_kernel_git_linux_kernel-vmlinux=/tmp/vmlinux"
echo ""
echo "TIP: you can watch for changes to specific kernel sources and rebuild automatically using godemon:"
echo "    go install github.com/bduffany/godemon/cmd/godemon@latest"
echo "    ( cd ../linux && godemon -w linux/drivers/block/nbd.c [ -w other_sources ... ] ../buildbuddy/build_kernel.sh )"

