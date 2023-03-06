#!/bin/bash
set -euo pipefail

: "${BB_SQLITE_DB_PATH:=/tmp/buildbuddy-enterprise.db}"

: "${GROUP_ID:=}"
: "${REPO_URL:=https://github.com/buildbuddy-io/buildbuddy}"
: "${GITHUB_TOKEN?}"
: "${BRANCH:=$(git rev-parse --abbrev-ref HEAD)}"
: "${COMMIT_SHA:=$(git rev-parse "$BRANCH")}"

sql() {
  sqlite3 "$BB_SQLITE_DB_PATH" "$@"
}

if ! [[ "$GROUP_ID" ]]; then
  # shellcheck disable=SC2016
  GROUP_ID=$(sql 'SELECT group_id FROM APIKeys WHERE user_id = "" OR user_id IS NULL LIMIT 1;')
fi

# Insert an artificial workflow row into the DB
webhook_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
WFID="WF$RANDOM$RANDOM"
sql "
INSERT INTO Workflows
  (workflow_id, group_id, webhook_id, repo_url, access_token, perms)
VALUES
  ('$WFID', '$GROUP_ID', '$webhook_id', '$REPO_URL', '$GITHUB_TOKEN', 0x770);
"
cleanup() {
  sql "
  DELETE FROM Workflows WHERE workflow_id = '$WFID'
  "
}
trap cleanup EXIT

cat >/tmp/webhook_data.json <<EOF
{
  "ref": "refs/heads/$BRANCH",
  "head_commit": {"id": "$COMMIT_SHA"},
  "repository": {"clone_url": "$REPO_URL", "private": false}
}
EOF

curl \
  -X POST \
  "http://localhost:8080/webhooks/workflow/$webhook_id" \
  --header 'X-Github-Event: push' \
  --header 'Content-Type: application/json' \
  --data @/tmp/webhook_data.json
echo

