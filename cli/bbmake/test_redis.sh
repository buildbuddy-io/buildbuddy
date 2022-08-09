#!/usr/bin/env bash
set -euo pipefail

: "${REDIS_REPO_DIR:=/tmp/redis}"
: "${BB_DIR:=$PWD}"

bazel build cli/cmd/bb

if ! [[ -e "$REDIS_REPO_DIR" ]]; then
  git clone https://github.com/redis/redis "$REDIS_REPO_DIR"
fi
cd "$REDIS_REPO_DIR"
cd src


export PATH="/opt/homebrew/opt/make/libexec/gnubin:$PATH"

make clean

clear
"$BB_DIR"/bazel-bin/cli/cmd/bb/bb_/bb make redis-server "$@"

