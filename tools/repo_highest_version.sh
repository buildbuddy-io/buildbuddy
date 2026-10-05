#!/usr/bin/env bash
set -euo pipefail

# Prints the highest BuildBuddy version tag in the repo, like "v2.12.8".
#
# Considers every tag, including tags on other branches, sorted by version
# rather than creation date, so a patch to an older release branch doesn't
# count as the newest version. New minor versions are bumped from this. See
# branch_highest_version.sh for the version of the current checkout.
#
# Only reads tags, not history, so it works in a shallow clone as long as tags
# have been fetched. Exits non-zero, printing nothing, if there's no version tag.

tag=$(git tag -l 'v*' --sort=version:refname |
  perl -nle 'if (/^v\d+\.\d+\.\d+$/) { print $_ }' |
  tail -n1)

if [[ -z "$tag" ]]; then
  echo "repo_highest_version.sh: no vX.Y.Z tag found." >&2
  exit 1
fi
echo "$tag"
