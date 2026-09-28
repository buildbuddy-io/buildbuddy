#!/usr/bin/env bash
set -e

# Prints the highest BuildBuddy version tag reachable from HEAD, like "v2.12.8".
#
# Only tags in HEAD's history are considered, so tags cut on other branches
# (e.g. a newer release branch) don't change the version stamped into builds
# of this commit.
#
# Exits non-zero if the version can't be determined reliably: in a shallow
# clone (tags may be missing from the truncated history) or when no version
# tag is reachable.

if [[ "$(git rev-parse --is-shallow-repository)" == "true" ]]; then
  echo "latest_version_tag.sh: shallow clone; fetch full history (e.g. fetch-depth: 0) to resolve the version tag." >&2
  exit 1
fi

tag=$(git tag --merged HEAD -l 'v*' --sort=v:refname |
  perl -nle 'if (/^v\d+\.\d+\.\d+$/) { print $_ }' |
  tail -n1)

if [[ -z "$tag" ]]; then
  echo "latest_version_tag.sh: no vX.Y.Z tag is reachable from HEAD." >&2
  exit 1
fi
echo "$tag"
