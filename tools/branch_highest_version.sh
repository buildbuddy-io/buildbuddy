#!/usr/bin/env bash
set -euo pipefail

# Prints the highest BuildBuddy version tag reachable from HEAD, like "v2.12.8".
#
# Unlike latest_version_tag.sh, which looks at every tag in the repo, this only
# considers tags in HEAD's history, so tags cut on other branches (e.g. a newer
# release branch) don't affect the result. On a release branch, this is the
# branch's own version.
#
# Exits non-zero, printing nothing, if the version can't be determined
# reliably: in a shallow clone (tagged commits may be missing from the
# truncated history) or when no version tag is reachable.

if [[ "$(git rev-parse --is-shallow-repository)" == "true" ]]; then
  echo "branch_highest_version.sh: shallow clone; fetch full history (e.g. fetch-depth: 0) to find the version tag." >&2
  exit 1
fi

tag=$(git tag -l 'v*' --merged HEAD --sort=version:refname |
  perl -nle 'if (/^v\d+\.\d+\.\d+$/) { print $_ }' |
  tail -n1)

if [[ -z "$tag" ]]; then
  echo "branch_highest_version.sh: no vX.Y.Z tag is reachable from HEAD." >&2
  exit 1
fi
echo "$tag"
