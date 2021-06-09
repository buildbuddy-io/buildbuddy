#!/bin/bash
set -e

declare -a secrets=("BB_DEV_GITHUB_CLIENT_ID" "BB_DEV_GITHUB_CLIENT_SECRET" "BB_DEV_OAUTH_CLIENT_ID" "BB_DEV_OAUTH_CLIENT_SECRET")

echo "Run the following commands to set your env vars:"
for s in "${secrets[@]}"; do
   echo "  export ${s}=$(gcloud secrets versions access latest --secret=${s})"
done
