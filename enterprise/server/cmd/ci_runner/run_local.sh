#!/bin/bash
set -euo pipefail

USAGE="Runs the action runner against a local git repo.

This is mainly useful for testing the action runner locally without needing
to manually craft the exact webhook payloads that would be needed to trigger
it correctly.

Examples:
  # Run actions for the BuildBuddy repo
  $0

  # Run actions for a local repo
  REPO=~/src/scratch/hello_world $0

  # Override action runner args
  $0 --bes_backend=grpcs://cloud.buildbuddy.io --bes_results_url=https://app.buildbuddy.io/invocation/
"
if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
  echo "$USAGE"
  exit
fi

# cd to workspace root
while ! [ -e ".git" ]; do cd ..; done

: "${REPO:=$PWD}"

bazel build //enterprise/server/cmd/ci_runner
runner=$(realpath ./bazel-bin/enterprise/server/cmd/ci_runner/ci_runner_/ci_runner)
cd "$(mktemp -d)"
"$runner" \
  --repo_url="file://$(realpath "$REPO")" \
  --commit_sha="$(cd "$REPO" && git rev-parse HEAD)" \
  --trigger_event=push \
  --trigger_branch="$(cd "$REPO" && git branch --show-current)" \
  --bes_backend=grpc://localhost:1985 \
  --bes_results_url=http://localhost:8080/invocation/ \
  "$@"
