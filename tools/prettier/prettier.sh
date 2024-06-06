#!/bin/bash
# Runs prettier on all files that differ from the main branch.

set -e

: "${PRETTIER_PATH:=}"

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
  '*.xml'
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
  if [[ "$(git rev-parse --abbrev-ref HEAD)" == master ]]; then
    DIFF_BASE='HEAD~1'
  else
    DIFF_BASE=$(git merge-base HEAD master)
  fi
  git diff --name-only --diff-filter=AMRCT "$DIFF_BASE" -- "${GIT_FILE_PATTERNS[@]}"
}

mapfile -t paths < <(paths_to_format)

if [[ -z "${paths[*]}" ]]; then
  exit 0
fi

# Run bazel quietly; see: https://github.com/bazelbuild/bazel/issues/4867#issuecomment-796208829
if [[ "$PRETTIER_PATH" ]]; then
  PRETTIER_COMMAND=("$PRETTIER_PATH")
else
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  bazel run @npm//prettier/bin:prettier --script_path="$tmp/run.sh" &>"$tmp/build.log" || {
    cat "$tmp/build.log" >&2
    exit 1
  }
  chmod +x "$tmp/run.sh"
  PRETTIER_COMMAND=("$tmp/run.sh" --bazel_node_working_dir="$PWD")
fi

"${PRETTIER_COMMAND[@]}" "${paths[@]}" "$@"
