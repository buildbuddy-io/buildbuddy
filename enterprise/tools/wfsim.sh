#!/bin/bash
set -euo pipefail

# TODO: move to bb repo
source /home/b/scripts/flags.sh
flag.string repo='https://github.com/buildbuddy-io/buildbuddy' 'Git repository URL'
flag.string branch='master' 'Git branch'
flag.string api_key 'BB API key'
flag.int commits=16 'Number of commits to run workflows on'
flag.string target='grpc://localhost:1985' 'Executor target'
flag.string bazel_command='test //... --config=remote-dev' 'Bazel command to run'
flag.bool dry_run 'Print commands without executing'
flag.parse

: "${CLI:=bazelisk run -- //cli}"

# Get latest commits.
SHORT_REPO=$(echo "$FLAG_repo" | perl -pe 's@^https://github.com/(.*)$@\1@')

echo >&2 "Fetching $FLAG_commits commits for $SHORT_REPO"
mapfile -t COMMITS < <(curl -fsSL "https://api.github.com/repos/$SHORT_REPO/commits" |
  jq -r '.[].sha' |
  head -n"$FLAG_commits" |
  tac)

RUN_ID="workflowsim.$(date "+%Y-%m-%d.%I:%M:%S%p")"
echo "Running with PATTERN metadata: $RUN_ID"

function run() {
  if ((FLAG_dry_run)); then
    echo "Would run:"
    for arg in "$@"; do
      echo "  ${arg@Q} \\"
    done
  else
    "$@"
  fi
}

INSTANCE_NAME="$(date +%s).$RANDOM.$RANDOM"

for commit in "${COMMITS[@]}"; do
  run $CLI \
    --verbose=1 \
    execute \
    --remote_header=workload-isolation-type=firecracker \
    --exec_properties=recycle-runner=true \
    --exec_properties=Pool=workflows \
    --exec_properties=use-self-hosted-executors=false \
    --exec_properties="container-image=docker://gcr.io/flame-public/rbe-ubuntu20-04-workflows@sha256:eb81189af1cec44b5348751f75ec7aea516d0de675b8461235f4cc4e553a34b5" \
    --remote_header=x-buildbuddy-api-key="$FLAG_api_key" \
    --remote_executor="$FLAG_target" \
    --remote_instance_name="$INSTANCE_NAME" \
    --action_env=GIT_BRANCH="$FLAG_branch" \
    -- \
    bash -c "
      set -e
      cd ~
      if ! [[ -e ./repo ]]; then
        git clone --filter=blob:none '$FLAG_repo' repo
      fi
      cd repo
      git fetch origin
      git checkout '$commit'
      bazelisk $FLAG_bazel_command \\
        --build_metadata=PATTERN=$RUN_ID \\
        --bes_backend=$FLAG_target \\
        --remote_cache=$FLAG_target \\
        --remote_executor=$FLAG_target \\
        --bes_results_url=https://app.buildbuddy.dev/invocation/ \\
        --remote_header=x-buildbuddy-api-key=$FLAG_api_key
    "
done
