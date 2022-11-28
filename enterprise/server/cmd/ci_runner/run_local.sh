#!/bin/bash
set -euo pipefail

USAGE="
Usage:  [REPO_PATH=/path/to/local/repo] $0 [ci_runner_args ...]

Runs configured workflows for a repo using the same environment that the
CI runner uses in prod.

This is useful for testing the CI runner locally without needing
to create real workflows and trigger the workflows via real webhook events.

Examples:
  # Run workflows for the local BuildBuddy repo
  $0

  # Run workflows for a different local repo
  REPO_PATH=~/src/scratch/hello_world $0

  # Run a workflow simulating a pull request to 'foo/bar:master' from 'fork/bar:feature'
  $0 --target_repo_url=https://github.com/foo/bar --target_branch=master --pushed_repo_url=https://github.com/FORK/bar --pushed_branch=feature

  # Override arbitrary CI runner args
  $0 --bes_backend=grpcs://remote.buildbuddy.io --bes_results_url=https://app.buildbuddy.io/invocation/
"
if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
  echo "$USAGE"
  exit
fi

# cd to workspace root
cd "$(dirname "$0")"
while ! [ -e "WORKSPACE" ]; do
  cd ..
  if [[ "$PWD" == / ]]; then
    echo >&2 "Failed to find the bazel workspace root containing this script."
    exit 1
  fi
done

dir_abspath() (cd "$1" && pwd)

# CI runner bazel cache is set to a fixed directory in order
# to speed up builds, but note that in production we don't yet
# have persistent local caching.
TEMPDIR=$(mktemp --dry-run | xargs dirname)
: "${CI_RUNNER_BAZEL_CACHE_DIR:=$TEMPDIR/buildbuddy_ci_runner_bazel_cache}"

: "${REPO_PATH:=$PWD}"
: "${PERSISTENT:=true}"
: "${BUILDBUDDY_API_KEY:=}"

bazel build //enterprise/server/cmd/ci_runner:buildbuddy_ci_runner
RUNNER_PATH="$PWD/bazel-bin/enterprise/server/cmd/ci_runner/buildbuddy_ci_runner"
RUNNER_DATA_DIR="$TEMPDIR/buildbuddy_ci_runner_data"
mkdir -p "$RUNNER_DATA_DIR"
rm -f "$RUNNER_DATA_DIR/ci_runner"
cp "$RUNNER_PATH" "$RUNNER_DATA_DIR/ci_runner"
echo "Copied ci_runner to $RUNNER_DATA_DIR/ci_runner"

mkdir -p "$CI_RUNNER_BAZEL_CACHE_DIR"

if ! docker inspect buildbuddy-ci-runner-local &>/dev/null; then
  # Initialize container
  docker run \
    --volume "$RUNNER_DATA_DIR:/runner-data" \
    --volume "$CI_RUNNER_BAZEL_CACHE_DIR:/root/.cache/bazel" \
    --volume "$(dir_abspath "$REPO_PATH"):/root/mounted_repo" \
    --net host \
    --detach \
    --rm \
    --name buildbuddy-ci-runner-local \
    gcr.io/flame-public/buildbuddy-ci-runner:v2.3.0 \
    sleep infinity
fi

docker exec \
  --interactive --tty \
  --env BUILDBUDDY_API_KEY="$BUILDBUDDY_API_KEY" \
  buildbuddy-ci-runner-local \
  /runner-data/ci_runner \
  --pushed_repo_url="file:///root/mounted_repo" \
  --target_repo_url="file:///root/mounted_repo" \
  --commit_sha="$(cd "$REPO_PATH" && git rev-parse HEAD)" \
  --pushed_branch="$(cd "$REPO_PATH" && git branch --show-current)" \
  --target_branch="master" \
  --trigger_event=pull_request \
  --bes_results_url=http://localhost:8080/invocation/ \
  --bes_backend=grpc://localhost:1985 \
  --cache_backend=grpc://localhost:1985 \
  --rbe_backend=grpc://localhost:1985 \
  --workflow_id=WF1234 \
  "$@"

if [[ "$PERSISTENT" != true ]]; then
  docker rm -f buildbuddy-ci-runner-local
fi
