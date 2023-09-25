#!/bin/bash
set -euo pipefail

# Launcher script to build and run the executor locally.
#
# Options:
#    -b, --bare          enable bare execution
#    -p, --podman        enable podman instead of docker (Linux only)
#    -f, --firecracker   enable firecracker and runs the executor as root (Linux only)
#
# Other arguments are forwarded to 'bazel run'.
#
# Environment variables:
#    BAZEL               bazel binary, e.g. bazelisk or bb (defaults to 'bazel')

: "${BAZEL:=bazel}"

# Parse launcher args
FIRECRACKER=0
PODMAN=0
BARE=0
ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
  -b | --bare)
    BARE=1
    ;;
  -p | --podman)
    PODMAN=1
    ;;
  -f | --firecracker)
    FIRECRACKER=1
    ;;
  *)
    ARGS+=("$1")
    ;;
  esac
  shift
done

# Split bazel args and binary args
source tools/bash/arrays.bash
split_args -- "${ARGS[@]}"
BAZEL_ARGS=("${BEFORE_SEPARATOR[@]}")
BINARY_ARGS=("${AFTER_SEPARATOR[@]}")

if ((BARE)); then
  BINARY_ARGS+=("--executor.enable_bare_runner=true")
fi
if ((PODMAN)); then
  BINARY_ARGS=("--executor.docker_socket=" "${BINARY_ARGS[@]}")
  BINARY_ARGS+=("--executor.enable_podman=true")
fi
if ((FIRECRACKER)); then
  if ! (command -v firecracker && command -v jailer) >/dev/null; then
    # TODO: have the executor get these from Bazel runfiles instead of PATH.
    echo >&2 "Missing firecracker or jailer in \$PATH. Run ./tools/enable_local_firecracker.sh"
    exit 1
  fi
  echo >&2 "NOTE: --firecracker requires sudo (password may be required)"
  BAZEL_ARGS+=("--run_under=sudo")
  BINARY_ARGS+=("--executor.enable_firecracker=true")
fi

exec $BAZEL run //enterprise/server/cmd/executor "${BAZEL_ARGS[@]}" -- "${BINARY_ARGS[@]}"
