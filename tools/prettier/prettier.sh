#!/bin/bash
# Runs prettier on all files that differ from the main branch.

set -e

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
  if (git diff "$DIFF_BASE" -- yarn.lock | grep -q prettier) || (git diff "$DIFF_BASE" -- .prettierrc | grep -q .); then
    git ls-files -- "${GIT_FILE_PATTERNS[@]}"
  else
    git diff --name-only --diff-filter=AMRCT "$DIFF_BASE" -- "${GIT_FILE_PATTERNS[@]}"
  fi
}

paths=()
while read -r path; do
  paths+=("$path")
done < <(paths_to_format)

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
