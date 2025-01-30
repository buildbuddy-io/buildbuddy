#!/bin/bash
# Runs goimports from the project root directory.

set -euo pipefail

if [[ -z "${RUNFILES_DIR+x}" ]]; then
  export RUNFILES_DIR="$PWD"/..
fi

if [[ -n "${BUILD_WORKSPACE_DIRECTORY}" ]]; then
  cd "$BUILD_WORKSPACE_DIRECTORY"
fi

: "${GOIMPORTS_PATH:=}"

if [[ "$GOIMPORTS_PATH" ]]; then
  GOIMPORTS_COMMAND=("$GOIMPORTS_PATH")
else
  GOIMPORTS_COMMAND=("${RUNFILES_DIR}/org_golang_x_tools/cmd/goimports/goimports_/goimports")
fi

"${GOIMPORTS_COMMAND[@]}" "$@"
