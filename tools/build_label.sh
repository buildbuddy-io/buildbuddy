#!/usr/bin/env bash
set -euo pipefail

# Usage: build_label.sh
#
# Prints the per-commit build label for HEAD: "<prefix>-YYYYMMDDTHHMMSS-<sha7>",
# where the date and time are HEAD's committer date in UTC. The prefix says where
# the build came from:
#   ci:    env CI is "true" and the tracked files match HEAD (an official build).
#   local: anything else.
#
# The label is deterministic per commit and prefix (it doesn't depend on build
# time). The non-numeric prefix keeps it from parsing as a semver version.
# It ignores tags.
#
# buildbuddy-internal's workspace_status.sh calls this script to stamp hosted
# builds, so the format must not change casually. buildbuddy's own builds still
# stamp the highest release tag reachable from HEAD (workspace_status.sh).
#
# Exits non-zero, printing nothing, if HEAD doesn't exist.

sha=$(git rev-parse --verify -q HEAD) || {
  echo "build_label.sh: HEAD does not exist." >&2
  exit 1
}

date=$(TZ=UTC git show -s --date=format-local:%Y%m%dT%H%M%S --format=%cd HEAD)

# Refresh the index so files that were only touched don't count as modified.
git update-index -q --refresh || true
prefix=local
if [[ "${CI:-}" == "true" ]] && git diff-index --quiet HEAD --; then
  prefix=ci
fi
echo "${prefix}-${date}-${sha:0:7}"
