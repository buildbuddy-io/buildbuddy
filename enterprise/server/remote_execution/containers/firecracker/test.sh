#!/usr/bin/env bash
set -e

# Firecracker tests have to be run as root since they use FUSE mounts.
# This is a convenience script intended for local development, which
# builds the test as the current user and runs it as root.
#
# Bazel args can be provided to this script.
# Args after "--" are passed to the 'go test' command, e.g. "-test.run=MyTestFilter"

# BAZEL env var: can be overridden to e.g. 'bb' or 'bazel --startup_opt=...'
: "${BAZEL:=bazel}"
# RUN_UNDER: test wrapper. 'sudo strace -f' is also useful.
: "${RUN_UNDER:=sudo}"
: "${TARGET:=firecracker_test_blockio}"

BAZEL_ARGS=()
# Read test 'args' attribute into GO_TEST_ARGS array
mapfile -t GO_TEST_ARGS < <(
  ./tools/buildozer.sh -output_json 'print args' \
    "//enterprise/server/remote_execution/containers/firecracker:$TARGET" |
    jq -r '.records[0].fields[0].list.strings[]'
)

# Split bazel args from binary args.
IN_TEST_ARGS=0
for arg in "$@"; do
  if ((IN_TEST_ARGS)); then
    GO_TEST_ARGS+=("$arg")
  elif [[ "$arg" == "--" ]]; then
    IN_TEST_ARGS=1
  else
    BAZEL_ARGS+=("$arg")
  fi
done

$BAZEL \
  build "//enterprise/server/remote_execution/containers/firecracker:$TARGET" \
  "${BAZEL_ARGS[@]}"

echo "Running test as root (may ask for password)"
$RUN_UNDER \
  "./bazel-bin/enterprise/server/remote_execution/containers/firecracker/${TARGET}_/$TARGET" \
  "${GO_TEST_ARGS[@]}"
