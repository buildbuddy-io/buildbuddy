#!/bin/bash
set -euo pipefail

# Runs a version of the CLI built from origin/master.
#
# To avoid adding excessive latency, it pulls from the remote repo and rebuilds
# the CLI at most once every 4 hours. This can be configured with
# BB_DEV_STALENESS_SECONDS.

: "${BB_DEV_STALENESS_SECONDS:=14400}" # 4 hours
: "${BB_DEV_BUILD_DIR:=/tmp/bb-dev-build}"

SCRIPT_NAME="$0"
BAZEL_COMMAND=$( (which bazelisk bazel || true) | head -n1)

mkdir -p "$BB_DEV_BUILD_DIR"

get_last_build_time() (
  cd "$BB_DEV_BUILD_DIR"
  if ! [[ -e ./bb ]] || ! [[ -e ./timestamp ]]; then
    printf "0"
    return
  fi
  cat ./timestamp
)

LAST_BUILD_TIME=$(get_last_build_time)
NOW=$(date +%s)

rebuild() (
  echo >&2 "$SCRIPT_NAME: building bb cli from source"
  echo >&2 "---"

  # Copy the BB repo to /tmp to avoid disrupting the current repo.
  cd "$BB_DEV_BUILD_DIR"

  if ! [[ -e buildbuddy ]]; then
    git clone https://github.com/buildbuddy-io/buildbuddy
  fi
  cd buildbuddy
  git pull
  "$BAZEL_COMMAND" build cli
  echo >&2 "---"

  cd "$BB_DEV_BUILD_DIR"

  rm -f ./bb &>/dev/null || true
  cp buildbuddy/bazel-bin/cli/cmd/bb/bb_/bb ./bb

  printf '%d' "$(date +%s)" >./timestamp
)

if ((NOW - LAST_BUILD_TIME > BB_DEV_STALENESS_SECONDS)); then
  rebuild
fi

exec "$BB_DEV_BUILD_DIR/bb" "$@"
