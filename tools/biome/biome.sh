#!/usr/bin/env bash
set -euo pipefail

# See: https://www.stevenengelhardt.com/2023/02/27/practical-bazel-using-bazels-bash-runfiles-library/

# --- begin runfiles.bash initialization v3 ---
# Copy-pasted from the Bazel Bash runfiles library v3.
set -uo pipefail
set +e
f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null ||
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null ||
  source "$0.runfiles/$f" 2>/dev/null ||
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null ||
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null ||
  {
    echo >&2 "ERROR: cannot find $f"
    exit 1
  }
f=
set -e
# --- end runfiles.bash initialization v3 ---

BIOME_BIN=$(rlocation buildbuddy/tools/biome/biome_crossplatform)
cd "$BUILD_WORKSPACE_DIRECTORY"
exec "$BIOME_BIN" "$@"
