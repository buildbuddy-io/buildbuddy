#!/usr/bin/env bash
set -euo pipefail

# Prints the per-commit build label for HEAD: "<prefix>-YYYYMMDD-<sha7>", where
# the date is HEAD's committer date in UTC. The non-numeric prefix keeps the
# label from parsing as a semver version, and says where the build came from:
#   ci:    env CI is "true" and the tracked files match HEAD (an official build).
#   local: anything else.
#
# The label is deterministic per commit and prefix (UTC committer date plus
# 7-char SHA; it doesn't depend on build time), and ignores tags.
# buildbuddy-internal's workspace_status.sh calls this script, so the format
# must not change casually.
#
# Exits non-zero, printing nothing, if HEAD doesn't exist.

sha=$(git rev-parse --verify -q HEAD) || {
  echo "build_label.sh: HEAD does not exist." >&2
  exit 1
}
date=$(TZ=UTC git show -s --date=format-local:%Y%m%d --format=%cd HEAD)

# Refresh the index so files that were only touched don't count as modified.
git update-index -q --refresh || true
prefix=local
if [[ "${CI:-}" == "true" ]] && git diff-index --quiet HEAD --; then
  prefix=ci
fi
echo "${prefix}-${date}-${sha:0:7}"
