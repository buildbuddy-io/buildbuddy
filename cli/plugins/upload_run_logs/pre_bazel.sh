#!/usr/bin/env bash
set -eu

# Exit early if this is not a 'bazel run' command
COMMAND=$(grep -v '^--' "$1" | head -n 1)
if [[ "$COMMAND" != "run" ]]; then
    exit 0
fi

BES_BACKEND=$(
  grep -oE -- '--bes_backend=[^[:space:]]+' "$1" |
  sed 's/^--bes_backend=//' |
  tail -n1
)

API_KEY=$(
  grep -oE -- '--remote_header=x-buildbuddy-api-key=[^[:space:]]+' "$1" |
  sed 's/^--remote_header=x-buildbuddy-api-key=//' |
  tail -n1
)

IID=$(uuidgen)

echo "--run_under=//cli/plugins/upload_run_logs:upload_run_logs --target=$BES_BACKEND --api_key=$API_KEY --invocation_id=$IID" >> "$1"
echo "--invocation_id=$IID" >>"$1"
