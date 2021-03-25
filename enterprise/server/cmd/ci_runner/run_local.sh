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

# CI runner bazel cache can be set to a fixed directory in order
# to speed up builds, but note that in production we don't yet
# have persistent local caching.
: "${CI_RUNNER_BAZEL_CACHE_DIR:=$(mktemp -d)}"
: "${REPO:=$PWD}"
: "${BRANCH=$(cd "$REPO" && git branch --show-current)}"
: "${TRIGGER_BRANCH:=master}"

bazel build //enterprise/server/cmd/ci_runner
runner=$(realpath ./bazel-bin/enterprise/server/cmd/ci_runner/ci_runner_/ci_runner)

mkdir -p "$CI_RUNNER_BAZEL_CACHE_DIR"

docker run \
  --volume "$runner:/bin/ci_runner" \
  --volume "$CI_RUNNER_BAZEL_CACHE_DIR:/root/.cache/bazel" \
  --volume "$(realpath "$REPO"):/root/mounted_repo" \
  --interactive \
  --tty \
  --net host \
  --rm \
  gcr.io/flame-public/buildbuddy-ci-runner:v1.7.1 \
  ci_runner \
  --repo_url="file:///root/mounted_repo" \
  --commit_sha="$(cd "$REPO" && git rev-parse HEAD)" \
  --trigger_event=push \
  --branch="$BRANCH" \
  --trigger_branch="$TRIGGER_BRANCH" \
  --bes_backend=grpc://localhost:1985 \
  --bes_results_url=http://localhost:8080/invocation/ \
  "$@"
