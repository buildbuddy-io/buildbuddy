#!/usr/bin/env bash
set -e

# Prints the BuildBuddy version tag pointing at HEAD, like "v2.12.8".
#
# Release workflows run this before building stamped artifacts. It fails if
# HEAD has no version tag (e.g. a commit was cherry-picked onto the release
# branch without tagging it), or if the version that would be stamped into
# binaries (tools/latest_version_tag.sh) doesn't match the tag at HEAD.

tag=$(git tag --points-at HEAD -l 'v*' --sort=v:refname |
  perl -nle 'if (/^v\d+\.\d+\.\d+$/) { print $_ }' |
  tail -n1)

if [[ -z "$tag" ]]; then
  echo "version_tag_at_head.sh: HEAD ($(git rev-parse --short HEAD)) has no vX.Y.Z tag." >&2
  echo "If you cherry-picked onto a release branch, tag the new HEAD by running" >&2
  echo "'./release.py --bump_version_type=patch' from the release branch, then re-run the workflow." >&2
  exit 1
fi

stamped=$("$(dirname "$0")/latest_version_tag.sh")
if [[ "$stamped" != "$tag" ]]; then
  echo "version_tag_at_head.sh: HEAD is tagged $tag, but builds would be stamped with $stamped." >&2
  exit 1
fi
echo "$tag"
