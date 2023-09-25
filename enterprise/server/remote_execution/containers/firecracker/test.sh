#!/usr/bin/env bash
set -e

# blockio tests have to be run as root for now since they use FUSE mounts.
#
# This is a convenience script intended for local development, which builds
# the test as the current user but runs it as root.
#
# Bazel args can be provided to this script.
# Args after "--" are passed to the go test, e.g. "-test.run=MyTestFilter"

# BAZEL env var: can be overridden to e.g. 'bb' or 'bazel --startup_opt=...'
: "${BAZEL:=bazel}"
# RUN_UNDER: test wrapper. 'sudo strace -f' is also useful.
: "${RUN_UNDER:=sudo}"
: "${TARGET:=firecracker_test_blockio}"

source tools/bash/arrays.bash
split_args -- "$@"
BAZEL_ARGS=("${BEFORE_SEPARATOR[@]}")
GO_TEST_ARGS=("${AFTER_SEPARATOR[@]}")

GO_TEST_ARGS=(
  # Keep in sync with args in BUILD file
  "--executor.firecracker_enable_nbd=true"
  "--executor.firecracker_enable_merged_rootfs=true"
  "--executor.firecracker_enable_uffd=true"
  "--executor.enable_local_snapshot_sharing=true"
  "${GO_TEST_ARGS[@]}"
)

$BAZEL \
  build "//enterprise/server/remote_execution/containers/firecracker:$TARGET" \
  "${BAZEL_ARGS[@]}"

echo "Running test as root (may ask for password)"
$RUN_UNDER \
  "./bazel-bin/enterprise/server/remote_execution/containers/firecracker/${TARGET}_/$TARGET" \
  "${GO_TEST_ARGS[@]}"
