#!/bin/bash
set -euo pipefail

: "${KERNEL_DIR:=$HOME/code/bb/linux}"

cd "$KERNEL_DIR"

cp ~/code/bb/buildbuddy/enterprise/vmsupport/kernel/microvm-kernel-x86_64.config .config
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

