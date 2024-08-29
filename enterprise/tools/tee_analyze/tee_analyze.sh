#!/usr/bin/env bash
set -euo pipefail

: "${ADMIN_API_KEY?}"
: "${ADMIN_GROUP_ID?}"

F="/tmp/tee_analyze.$(date +%s%N).log"
rm -f /tmp/tee_analyze.latest.log || true
ln -s "$F" /tmp/tee_analyze.latest.log

bazel run \
  --bes_backend= \
  -- \
  enterprise/tools/tee_analyze:tee_analyze \
  --target=remote.buildbuddy.io \
  --log_lookback=16h \
  --ignore_isolation_type=firecracker \
  --ignore_docker_network=off \
  --admin_api_key="$ADMIN_API_KEY" \
  --admin_group_id="$ADMIN_GROUP_ID" \
  "$@" 2>&1 | tee "$F"
