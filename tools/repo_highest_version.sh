#!/usr/bin/env bash
set -euo pipefail

# Prints the highest BuildBuddy version tag in the repo, like "v2.12.8", not restricted to any branches.
# Requires tags to have been fetched first.

tag=$(git tag -l 'v*' --sort=version:refname |
  perl -nle 'if (/^v\d+\.\d+\.\d+$/) { print $_ }' |
  tail -n1)

if [[ -z "$tag" ]]; then
  echo "repo_highest_version.sh: no vX.Y.Z tag found." >&2
  exit 1
fi
echo "$tag"
