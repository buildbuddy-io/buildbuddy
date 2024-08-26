#!/usr/bin/env bash
set -euo pipefail
set -m

# Get execution IDs from this URL:
# http://go/bad-tasksize

: "${CLEAN:=0}"
: "${ENVFILE:=}"
if [[ ! "$ENVFILE" ]]; then
  ENVFILE=../replay.env
  touch "$ENVFILE"
elif ((CLEAN)); then
  trunc --size=0 "$ENVFILE"
fi
echo >&2 "Evaluating $ENVFILE"
cat "$ENVFILE" >&2
eval "$(cat "$ENVFILE")"

: "${COMPUTE_UNITS:=0}"
: "${PARALLEL_STRESS:=0}"
: "${TARGET_ENV:=dev}"
: "${SOURCE_ENV:=prod}"
: "${API_KEY=}"
: "${EXECUTION_ID=}"
: "${EXECUTOR_ID=}"
# Add groups to this file to exclude them.
: "${EXCLUDE_GROUPS_FILE=../exclude_groups.txt}"

query_logs() {
  echo >&2 '# Running query'
  echo >&2 "$1"
  gcloud logging read --limit="${LIMIT:-100}" --format=json "$1"
}

SOURCE=remote.buildbuddy.io
if [[ "$SOURCE_ENV" == dev ]]; then
  SOURCE=remote.buildbuddy.dev
fi
TARGET=remote.buildbuddy.dev
if [[ "$TARGET_ENV" == local ]]; then
  TARGET=grpc://localhost:1985
fi
if [[ "$TARGET_ENV" == prod ]]; then
  TARGET=remote.buildbuddy.io
fi

if [[ ! "$EXECUTOR_ID" ]] && [[ "$TARGET_ENV" != local ]]; then
  echo >&2 "# Getting an executor ID for replaying..."
  EXECUTOR_ID="$(LIMIT=1 query_logs '
timestamp>="'$(timeconv now-3h -t iso)'"
resource.labels.namespace_name="executor-'"$TARGET_ENV"'"
resource.labels.container_name="executor"
jsonPayload.executor_id!=""
' | jq -r '.[].jsonPayload.executor_id' | tail -n1)"
  echo "export EXECUTOR_ID=$EXECUTOR_ID" | tee -a "$ENVFILE"
fi
if [[ ! "$EXECUTION_ID" ]]; then
  echo >&2 "# Finding bad execution ID..."
  EXCLUDE_GROUPS="$(
    cat "$EXCLUDE_GROUPS_FILE" 2>/dev/null |
      xargs -r printf '-"%s"\n'
  )"
  LIMIT=10000 query_logs '
timestamp>="'$(timeconv now-12h -t iso)'"
resource.labels.namespace_name="buildbuddy-prod"
jsonPayload.message:"Computed CPU"
jsonPayload.message=~": \d{5,} average"
'"$EXCLUDE_GROUPS"' 
' | python3 ./get_bad_execution.py | tee -a "$ENVFILE"
fi

eval "$(cat "$ENVFILE")"

if [[ "$TARGET_ENV" == local ]]; then
  export EXECUTOR_ID=""
fi

ACTION_DIGEST="$(echo "$EXECUTION_ID" | perl -pe 's@^.*?(/blobs/)@\1@')"
INSTANCE_NAME="$(echo "$EXECUTION_ID" | perl -pe 's@^(.*?)/uploads/.*$@\1@')"

echo "ACTION_DIGEST=$ACTION_DIGEST"
echo "INSTANCE_NAME=$INSTANCE_NAME"

if [[ ! "$API_KEY" ]]; then
  echo "GROUP_ID=$GROUP_ID"
  echo >&2 "Missing API_KEY="
  exit 1
fi
# truncate --size=0 /tmp/responses.log

if ((PARALLEL_STRESS > 0)); then
  bb version
  : "${STRESS=--cpu=4 --io=4 --malloc=4 --hdd=4 --timeout=5s}"
  bash -c '
    while true; do
      echo /tmp/bb execute \
        --remote_executor='"$TARGET"' \
        --exec_properties=EstimatedComputeUnits='"$COMPUTE_UNITS"' \
        --exec_properties=container-image=docker://ecerulm/stress-ng \
        --exec_properties=debug-executor-id='"$EXECUTOR_ID"' \
        -- \
        stress-ng '"$STRESS"'
    done | parallel -j'"$PARALLEL_STRESS"'
  ' &
  PARALLEL_STRESS_PID=$!
  trap "kill -KILL -- -$PARALLEL_STRESS_PID" EXIT
fi

FLAGS=(
  --source_executor="$SOURCE"
  --source_api_key="$API_KEY"
  --source_remote_instance_name="$INSTANCE_NAME"
  --action_digest="$ACTION_DIGEST"
  --target_executor="$TARGET"
  --target_headers=x-buildbuddy-platform.debug-executor-id="$EXECUTOR_ID"
  --target_headers=x-buildbuddy-platform.EstimatedComputeUnits="$COMPUTE_UNITS"
  --jobs=120
  --n=240
  --response_log=/tmp/responses.log
)
bazel run -- \
  enterprise/tools/replay_action \
  "${FLAGS[@]}" "$@" 2>&1 | tee /tmp/replay_action.log >&2

python3 ./parse_response_log.py </tmp/responses.log
