#!/bin/bash
# Runs prettier on all files that differ from the main branch.

set -e

# Replacement for GNU realpath (not available on Mac)
realpath() {
  [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

NODE=$(realpath "${1?}")
shift
PRETTIER=$(realpath "${1?}")
shift

# note: BUILD_WORKSPACE_DIRECTORY points to the workspace root (containing all the
# source code) and is set by `bazel run`.
cd "${BUILD_WORKSPACE_DIRECTORY?}"

# Diagram showing what `git merge-base HEAD refs/remotes/origin/master` is doing:
#
# o <- fetched remote master branch (refs/remotes/origin/master)
# |
# o  o <- current local branch (HEAD)
# | /
# o <- $(git merge-base HEAD refs/remotes/origin/master)
# |
#
# We're finding the latest common ancestor between the remote master branch
# and the current branch, so that we only run the lint check on files added
# in this branch.

paths=("$(
  git merge-base HEAD refs/remotes/origin/master |
    xargs git diff --name-only --diff-filter=AMRCT |
    while read -r path; do
      if [[ "$path" =~ \.(js|jsx|ts|tsx|html|css|yaml|json|md|xml)$ ]]; then
        echo "$path"
      fi
    done
)")

if [[ -z "${paths[*]}" ]]; then
  exit 0
fi

"$NODE" "$PRETTIER" "$@" "${paths[@]}"
