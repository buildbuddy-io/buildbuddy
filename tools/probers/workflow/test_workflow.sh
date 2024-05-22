#!/usr/bin/env bash
set -e

app_target=$1
api_key=$2

execute_workflow_resp=$(curl -s -d \
'{ "repo_url":"https://github.com/buildbuddy-io/probers", "branch": "main", "action_names": ["Prober test"]}' \
-H "x-buildbuddy-api-key: $api_key" \
-H 'Content-Type: application/json'  \
"$app_target/api/v1/ExecuteWorkflow")
invocation_id=$(echo "$execute_workflow_resp" | jq -r '.actionStatuses[0].invocationId' )

# Poll until invocation is finished. Timeout after 5min
sleep_interval=15
for ((i = 0; i < 20; i++)); do
    invocation_resp=$(curl -s -d \
    "{ \"selector\": {\"invocation_id\": \"$invocation_id\"}}" \
    -H "x-buildbuddy-api-key: $api_key" \
    -H 'Content-Type: application/json'  \
    "$app_target/api/v1/GetInvocation")

    invocation_complete=$(echo "$invocation_resp" | jq '[.invocation[] | has("bazelExitCode")] | any')
    if [[ "$invocation_complete" == true ]]; then
      success=$(echo "$invocation_resp" | jq -r '.invocation[0].success')
      bazel_exit_code=$(echo "$invocation_resp" | jq -r '.invocation[0].bazelExitCode')

      if [[ "$success" == true && "$bazel_exit_code" == "OK" ]]; then
        echo "Success"
        exit 0
      else
        echo "$invocation_resp"
        exit 1
      fi
    fi

    # If the invocation hasn't completed yet, sleep and retry
    sleep $sleep_interval
done


exit 1
