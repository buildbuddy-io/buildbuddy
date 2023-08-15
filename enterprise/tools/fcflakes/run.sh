#!/usr/bin/env bash
set -e

# Run remote (dev) by default.
# Set LOCAL=1 to run locally
: "${LOCAL:=1}"

ARGS=()
if ((LOCAL)); then
  ARGS+=(
    --remote_executor=grpc://localhost:1985
  )
else
  ARGS+=(
    --remote_executor=remote.buildbuddy.dev
    --bes_backend=remote.buildbuddy.dev
    --bes_results_url=https://app.buildbuddy.dev/invocation/
  )
fi

exec bb test //enterprise/tools/fcflakes:repro \
  --runs_per_test=10000 \
  --test_keep_going \
  --test_output=errors \
  --ui_actions_shown=32 \
  "${ARGS[@]}" \
  "$@"
