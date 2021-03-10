#!/bin/bash
set -e

# Runs prettier on all files that differ from the main branch.

# note: BUILD_WORKSPACE_DIRECTORY points to the workspace root (containing all the
# source code) and is set by `bazel run`.
cd "${BUILD_WORKSPACE_DIRECTORY?}"

git diff --name-only --diff-filter=AMRCT master |
  grep -P '\.(js|jsx|ts|tsx|html|css|yaml|json|md|xml)$' |
  xargs --no-run-if-empty ./node_modules/.bin/prettier "$@"
