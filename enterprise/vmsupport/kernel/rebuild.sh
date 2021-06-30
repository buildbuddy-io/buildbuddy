#!/bin/bash
set -e

git clone --depth 1 --branch v5.4 git://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git
pushd linux
cp ../microvm-kernel-x86_64.config .config
make olddefconfig
make -j 16 vmlinux
cp vmlinux ../../bin/vmlinux
popd
