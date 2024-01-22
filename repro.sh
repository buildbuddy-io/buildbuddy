#!/usr/bin/env bash

: "${INIT_DOCKERD:=true}"
: "${API_KEY:=}"
: "${EXECUTOR:=grpc://localhost:1985}"
: "${IID:=$(uuidgen)}"

API_KEY_ARG=()
if [[ "$API_KEY" ]]; then
  API_KEY_ARG=(--remote_header=x-buildbuddy-api-key=$API_KEY)
fi



set -x
bb execute \
    --invocation_id="$IID" \
    --remote_executor="$EXECUTOR" \
    "${API_KEY_ARG[@]}" \
    --exec_properties=workload-isolation-type=firecracker \
    --exec_properties=recycle-runner=true \
    --exec_properties=init-dockerd=$INIT_DOCKERD \
    -- sh -c 'cat /tmp/recycled 2>/dev/null || echo "not recycled" ; echo recycled > /tmp/recycled'
