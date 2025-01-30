#!/bin/bash
# Runs goimports from the project root directory.

# --- begin runfiles.bash initialization v3 ---
# Copy-pasted from the Bazel Bash runfiles library v3.
set -uo pipefail; set +e; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v3 ---

if [[ -n "${BUILD_WORKSPACE_DIRECTORY}" ]]; then
  cd "$BUILD_WORKSPACE_DIRECTORY"
fi

: "${GOIMPORTS_PATH:=}"

if [[ "$GOIMPORTS_PATH" ]]; then
  GOIMPORTS_COMMAND=("$GOIMPORTS_PATH")
else
  GOIMPORTS_COMMAND=("$(rlocation org_golang_x_tools/cmd/goimports/goimports_/goimports)")
fi

"${GOIMPORTS_COMMAND[@]}" "$@"
