#!/usr/bin/env bash
set -euo pipefail

# Usage: build_label.sh [--ignore-tags]
#
# Prints the version label for HEAD. If tags matching vX.Y.Z point at HEAD, it
# is the highest of them: public release builds run at the tagged commit and
# get vX.Y.Z. Otherwise it is the per-commit label "<prefix>-YYYYMMDD-<sha7>",
# where the date is HEAD's committer date in UTC. The non-numeric prefix keeps
# the label from parsing as a semver version, and says where the build came
# from:
#   ci:    env CI is "true" and the tracked files match HEAD (an official build).
#   local: anything else.
#
# The per-commit label is deterministic per commit and prefix (UTC committer
# date plus 7-char SHA; it doesn't depend on build time).
#
# With --ignore-tags, always prints the per-commit label. buildbuddy-internal
# calls this script with --ignore-tags, because hosted builds must always get
# the per-commit label, even at a commit that was publicly tagged later;
# otherwise two builds of the same commit would disagree. The format must not
# change casually.
#
# Exits non-zero, printing nothing, if HEAD doesn't exist.

ignore_tags=false
if [[ $# -eq 1 && "$1" == "--ignore-tags" ]]; then
  ignore_tags=true
elif [[ $# -gt 0 ]]; then
  echo "Usage: build_label.sh [--ignore-tags]" >&2
  exit 2
fi

sha=$(git rev-parse --verify -q HEAD) || {
  echo "build_label.sh: HEAD does not exist." >&2
  exit 1
}

if [[ "$ignore_tags" == false ]]; then
  version_tag=$(git tag --points-at HEAD -l 'v*' --sort=version:refname |
    perl -nle 'if (/^v\d+\.\d+\.\d+$/) { print $_ }' |
    tail -n1)
  if [[ -n "$version_tag" ]]; then
    echo "$version_tag"
    exit 0
  fi
fi

date=$(TZ=UTC git show -s --date=format-local:%Y%m%d --format=%cd HEAD)

# Refresh the index so files that were only touched don't count as modified.
git update-index -q --refresh || true
prefix=local
if [[ "${CI:-}" == "true" ]] && git diff-index --quiet HEAD --; then
  prefix=ci
fi
echo "${prefix}-${date}-${sha:0:7}"
