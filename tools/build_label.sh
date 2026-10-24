#!/usr/bin/env bash
set -euo pipefail

# Prints the per-commit build label for HEAD: "build-YYYYMMDD-<sha7>", where the
# date is HEAD's committer date in UTC. The non-numeric prefix keeps the label
# from parsing as a semver version.
#
# The label is deterministic per commit (UTC committer date plus 7-char SHA; it
# doesn't depend on build time), and ignores tags. buildbuddy-internal's
# workspace_status.sh calls this script, so the format must not change casually.
#
# Exits non-zero, printing nothing, if HEAD doesn't exist.

sha=$(git rev-parse --verify -q HEAD) || {
  echo "build_label.sh: HEAD does not exist." >&2
  exit 1
}
date=$(TZ=UTC git show -s --date=format-local:%Y%m%d --format=%cd HEAD)
echo "build-${date}-${sha:0:7}"
