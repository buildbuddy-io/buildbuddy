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
BES_BACKEND_FLAG=""
if [[ "$BES_BACKEND" != "" ]]; then
  BES_BACKEND_FLAG=" --target=$BES_BACKEND"
fi

API_KEY=$(
  grep -oE -- '--remote_header=x-buildbuddy-api-key=[^[:space:]]+' "$1" |
  sed 's/^--remote_header=x-buildbuddy-api-key=//' |
  tail -n1
)
API_KEY_FLAG=""
if [[ "$API_KEY" != "" ]]; then
  API_KEY_FLAG=" --api_key=$API_KEY"
fi

IID=$(uuidgen)

# Ensure the build uses the invocation ID passed to the plugin.
echo "--invocation_id=$IID" >> "$1"
# Run the plugin.
echo "--run_under=//cli/plugins/upload_run_logs:upload_run_logs $BES_BACKEND_FLAG $API_KEY_FLAG --invocation_id=$IID" >> "$1"
