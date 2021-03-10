#!/bin/bash
set -e
# Runs prettier on all files that differ from the main branch.

: "${PRETTIER:=$(realpath ./external/npm/node_modules/prettier/bin-prettier.js)}"

# note: BUILD_WORKSPACE_DIRECTORY points to the workspace root (containing all the
# source code) and is set by `bazel run`.
cd "${BUILD_WORKSPACE_DIRECTORY?}"

# Diagram showing what `git merge-base HEAD master` is doing:
#
# o <- master
# |
# o  o <- HEAD
# | /
# o <- $(git merge-base HEAD master)
# |
#
# It allows us to compare against the master branch as it was
# before branching off into the current branch.

git merge-base HEAD master |
  xargs git diff --name-only --diff-filter=AMRCT |
  grep -P '\.(js|jsx|ts|tsx|html|css|yaml|json|md|xml)$' |
  xargs --no-run-if-empty "$PRETTIER" "$@"
