#!/usr/bin/env bash
set -euo pipefail

: "${API_KEY:?API_KEY must be set in the environment}"

ENDPOINT="https://app.buildbuddy.io/rpc/BuildBuddyService/CancelExecutions"

readarray -t INVOCATION_IDS <<'EOF'
INSERT HERE
EOF

success=0
failed=0

for invocation_id in "${INVOCATION_IDS[@]}"; do
  [[ -z "$invocation_id" ]] && continue

  printf 'Cancelling %s... ' "$invocation_id"
  response_file="$(mktemp)"

  if http_code="$(
    curl -sS \
      -o "$response_file" \
      -w '%{http_code}' \
      -H "x-buildbuddy-api-key: $API_KEY" \
      -H 'Content-Type: application/json' \
      -d "{\"invocationId\":\"$invocation_id\"}" \
      "$ENDPOINT"
  )"; then
    if [[ "$http_code" == "200" ]]; then
      echo "ok"
      ((success += 1))
    else
      echo "failed (HTTP $http_code)"
      cat "$response_file"
      ((failed += 1))
    fi
  else
    echo "curl failed"
    if [[ -s "$response_file" ]]; then
      cat "$response_file"
    fi
    ((failed += 1))
  fi

  rm -f "$response_file"
done

echo
echo "Finished. Successes: $success Failures: $failed"

[[ $failed -eq 0 ]]
