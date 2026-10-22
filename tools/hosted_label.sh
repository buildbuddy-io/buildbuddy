#!/usr/bin/env bash
set -euo pipefail

# Prints the hosted version label for HEAD: "hosted-YYYYMMDD-<sha7>", where the
# date is HEAD's committer date in UTC.
#
# The label is deterministic per commit (it doesn't depend on build time), and
# ignores tags. buildbuddy-internal's workspace_status.sh calls this script, so
# the format must not change casually.
#
# Exits non-zero, printing nothing, if HEAD doesn't exist.

sha=$(git rev-parse --verify -q HEAD) || {
  echo "hosted_label.sh: HEAD does not exist." >&2
  exit 1
}
date=$(TZ=UTC git show -s --date=format-local:%Y%m%d --format=%cd HEAD)
echo "hosted-${date}-${sha:0:7}"
