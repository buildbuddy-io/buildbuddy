#!/bin/bash
set -euo pipefail

response=$(curl -s -d '{ "repo_url":"https://github.com/buildbuddy-io/buildbuddy", "ref": "master", "action_names": ["Check style"]}' -H "x-buildbuddy-api-key: $DEV_API_KEY" -H 'Content-Type: application/json'  https://app.buildbuddy.dev/api/v1/ExecuteWorkflow)
invocation_id=$(echo "$response" | jq -r '.actionStatuses[0].invocationId')

for i in {1..10}; do
	response=$(curl -s https://app.buildbuddy.dev/rpc/BuildBuddyService/GetInvocation -H 'Content-Type: application/json' -H "x-buildbuddy-api-key: $DEV_API_KEY" --data "{\"lookup\": {\"invocation_id\": \""$invocation_id"\"}}")
	if [[ "$response" == *"record not found"* ]]; then
		sleep 30
	else
		echo "Successful invocation link: https://app.buildbuddy.dev/invocation/$invocation_id"
		exit 0
	fi
done

echo "Invocation link for failed workflow: https://app.buildbuddy.dev/invocation/$invocation_id?runnerFailed=true"
