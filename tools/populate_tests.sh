#!/usr/bin/env bash
set -euo pipefail

# Populates test history for a local BB org.
#
# Be sure to run the BB server with ClickHouse enabled, e.g.:
#
#   bazel run //enterprise/server -- `tools/clickhouse`
#
# Then, get an API key from your local BB org, and run this script like:
#
#   API_KEY=XXXXXX ./tools/populate_tests.sh
#
# Environment variables:
#   API_KEY: local API key (required)
#   COMMITS: number of commits to create
#   CLEAN_DB: if set, truncate the TestTargetStatuses table before populating

: "${API_KEY?missing local API_KEY env var}"
: "${COMMITS:=10}"
: "${CLEAN_DB:=0}"

if (( CLEAN_DB )); then
  # Remove all existing test history from ClickHouse
  echo "TRUNCATE TABLE TestTargetStatuses;" | clickhouse-client -d buildbuddy_local
fi

cd "$(mktemp -d)"
WORKDIR="$PWD"
trap 'rm -rf "$WORKDIR"' EXIT

git init --initial-branch=main
touch MODULE.bazel
echo 8.1.1 > .bazelversion
echo > .bazelrc '
common --workspace_status_command=$(pwd)/workspace_status.sh
common:ci --build_metadata=ROLE=CI
'
echo >workspace_status.sh '#!/usr/bin/env bash
echo REPO_URL https://github.com/testorg/testgrid
echo COMMIT_SHA $(git rev-parse HEAD)
echo GIT_BRANCH $(git rev-parse --abbrev-ref HEAD)
' && chmod +x workspace_status.sh
echo >BUILD '
sh_test(
    name = "passing_test",
    srcs = ["exit_test.sh"],
)
sh_test(
    name = "failing_test",
    srcs = ["exit_test.sh"],
    env = {"EXIT_CODE": "1"},
)
sh_test(
    name = "flaky_test",
    srcs = ["flaky_test.sh"],
)
'
echo >exit_test.sh 'exit ${EXIT_CODE:-0}' && chmod +x exit_test.sh
echo >flaky_test.sh '(( RANDOM % 2 == 0 )) || exit 1' && chmod +x flaky_test.sh
git add .
git commit -m "Initial commit"

run_tests() {
  bazelisk test //... \
      --jobs=1 \
      --nocache_test_results \
      --bes_backend=grpc://localhost:1985 \
      --bes_results_url=http://localhost:8080/invocation/ \
      --remote_cache=grpc://localhost:1985 \
      --remote_header=x-buildbuddy-api-key="${API_KEY}" \
      --config=ci || true
}

# Add several new tests over the course of several commits,
# and run all tests at each commit.
for i in $(seq 1 $COMMITS); do
  # Check out feature branch,
  # add a feature, and run tests
  git checkout -b feature-${i}

  echo >> BUILD '
sh_test(
    name = "feature_'${i}'_test",
    srcs = ["exit_test.sh"],
    env = {"SALT": "'${i}'"},
)
'
  git add .
  git commit -m "Add feature $i"
  run_tests

  # Merge feature into main
  git checkout main
  git merge feature-${i}

  # Run tests again on main
  run_tests
done

