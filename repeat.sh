#!/bin/bash
while true
do
  # Sleep random amount between 1s and 5 min
  random_sleep=$((RANDOM % 300 + 1))
  echo $random_sleep
  curl -d '{"repo_url":"https://github.com/buildbuddy-io/buildbuddy", "ref": "workflow_bare_pool", "action_names": ["Test"]}' -H "x-buildbuddy-api-key: $API_KEY" -H 'Content-Type: application/json'  https://app.buildbuddy.dev/api/v1/ExecuteWorkflow
  sleep "$random_sleep"
done
