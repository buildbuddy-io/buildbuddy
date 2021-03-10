#!/bin/bash
set -e

# note: BUILD_WORKSPACE_DIRECTORY points to the workspace root (containing all the source code)
# and is set by `bazel run`.
cd "${BUILD_WORKSPACE_DIRECTORY?}"

git diff --name-only --diff-filter=AMRCT |
  grep -P '\.(js|jsx|ts|tsx|html|css|yaml|json|md|xml)$' |
  xargs --no-run-if-empty ./node_modules/.bin/prettier "$@"
