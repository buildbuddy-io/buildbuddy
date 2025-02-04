#!/bin/bash
# Runs prettier on all files that differ from the main branch.

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

if [[ -n "${BUILD_WORKSPACE_DIRECTORY}" ]]; then
  cd "$BUILD_WORKSPACE_DIRECTORY"
fi

: "${PRETTIER_PATH:=}"
: "${DIFF_BASE:=}"

# File patterns to format using prettier, using git's file pattern syntax.
GIT_FILE_PATTERNS=(
  '*.css'
  '*.html'
  '*.js'
  '*.json'
  '*.jsx'
  '*.md'
  '*.ts'
  '*.tsx'
  '*.yaml'
)

# Replacement for GNU realpath (not available on Mac)
realpath() {
  [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

# paths_to_format returns the files that have changed relative to the base
# branch. The base branch is computed as the latest common commit between
# the current branch commit (HEAD) and master.
#
# If we are on the master branch already, then look one commit back.
# This is needed to support workflows that run on push.
function paths_to_format() {
  if ! [[ "$DIFF_BASE" ]]; then
    if [[ "$(git rev-parse --abbrev-ref HEAD)" == master ]]; then
      DIFF_BASE='HEAD~1'
    else
      # If on a feature branch, use GIT_BASE_BRANCH env var set by BB workflows,
      # or fall back to master (which should usually work when running locally).
      DIFF_BASE=$(git merge-base HEAD "origin/${GIT_BASE_BRANCH:-master}")
    fi
  fi

  # If any of the following are changed, then all files need to be reformatted,
  # not just changed files:
  # - The prettier version in yarn.lock
  # - Any prettier configuration in .prettierrc
  if (git diff "$DIFF_BASE" -- yarn.lock | grep -q prettier) || (git diff "$DIFF_BASE" -- .prettierrc tools/prettier/prettier.sh | grep -q .); then
    git ls-files -- "${GIT_FILE_PATTERNS[@]}"
  else
    git diff --name-only --diff-filter=AMRCT "$DIFF_BASE" -- "${GIT_FILE_PATTERNS[@]}"
  fi
}

paths=()
format=0
while read -r path; do
  paths+=("$path")
  format=1
done < <(paths_to_format)

if [[ "$format" -eq 0 ]]; then
  exit 0
fi

if [[ "$PRETTIER_PATH" ]]; then
  PRETTIER_COMMAND=("$PRETTIER_PATH")
else
  PRETTIER_COMMAND=("$(rlocation npm/prettier/bin/prettier.sh)" --bazel_node_working_dir="$PWD")
fi

"${PRETTIER_COMMAND[@]}" "${paths[@]}" "$@"
