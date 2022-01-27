#!/bin/bash
set -euo pipefail

: "${GAZELLE_PATH:=}"

GAZELLE_COMMAND=(bazelisk run //:gazelle --)
if [[ "$GAZELLE_PATH" ]]; then
  GAZELLE_COMMAND=("$GAZELLE_PATH")
fi

DIFF_MODE=0
if [[ "${1:-}" == "-d" ]] || [[ "${1:-}" == "--diff" ]]; then
  DIFF_MODE=1
fi

if ((DIFF_MODE)); then
  if ! git diff --quiet; then
    echo >&2 "Git working tree is dirty. To run in diff mode, 'check_go_deps.sh' must be run from a clean tree."
    git status --short --untracked=no 1>&2
    exit 1
  fi
fi

tmp_log_file=$(mktemp)
cleanup() {
  if ((DIFF_MODE)); then
    git restore go.mod go.sum deps.bzl
  fi
  rm -r "$tmp_log_file"
}
trap cleanup EXIT

# go mod tidy fails if generated sources are not checked into the repo,
# and we don't want to require that (yet, at least). So use the `-e`
# option to ask `go mod tidy` to proceed even if it encounters errors
# loading packages.
if ! go mod tidy -e &>"$tmp_log_file"; then
  echo "Command 'go mod tidy -e' failed. Logs:" >&2
  cat "$tmp_log_file" >&2
  exit 1
fi

# Make sure go.mod has at most two `require()` blocks (direct imports then indirect imports).
# TODO(bduffany): Fix automatically.
require_block_count=$(grep -c '^require (' go.mod)
if ((require_block_count > 2)); then
  echo "Found more than two 'require(...)' sections in go.mod." >&2
  echo "Please fix by manually merging all require() blocks into a single block, then running tools/fix_go_deps.sh" >&2
  exit 1
fi

# Update deps.bzl (using Gazelle)
if ! "${GAZELLE_COMMAND[@]}" update-repos -from_file=go.mod \
  -to_macro=deps.bzl%install_buildbuddy_dependencies &>"$tmp_log_file"; then
  echo "Auto-updating 'deps.bzl' failed. Logs:" >&2
  cat "$tmp_log_file" >&2
  exit 1
fi

if ((DIFF_MODE)); then
  git diff
fi
