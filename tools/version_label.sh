#!/usr/bin/env bash
set -euo pipefail

# Prints the version label to stamp into a build of HEAD: the highest release
# tag (vX.Y.Z) pointing at HEAD if there is one, else the hosted label from
# hosted_label.sh. Only tags at HEAD itself count, not reachable ones.
#
# Exits non-zero, printing nothing, if HEAD doesn't exist.

tag=$(git tag --points-at HEAD -l 'v*' --sort=version:refname |
  perl -nle 'if (/^v\d+\.\d+\.\d+$/) { print $_ }' |
  tail -n1)

if [[ -n "$tag" ]]; then
  echo "$tag"
else
  "$(dirname "${BASH_SOURCE[0]}")/hosted_label.sh"
fi
