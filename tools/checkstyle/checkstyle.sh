#!/bin/bash
set -euo pipefail

export RUNFILES_DIR=$(cd ../ && pwd)

# Use absolute paths as we cd to the WORKSPACE directory below.
GO_PATH="$(pwd)/go"
GOFMT_PATH="$(pwd)/gofmt"
GOIMPORTS_PATH="$(pwd)/tools/goimports/goimports.sh"
BB_PATH="$(pwd)/cli/cmd/bb/bb_/bb"
PRETTIER_PATH="$(pwd)/tools/prettier/prettier.sh"
CLANG_FORMAT_PATH="$(pwd)/tools/clang-format/clang-format"

# Make sure 'go' is in $PATH (gazelle depends on this).
# TODO: set up the env properly to point to the bazel-provisioned SDK root
export PATH="$PATH:$PWD"

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

# Export the absolute path to the prettier plugin so that prettier.sh can find
# it when running under `bazel run`.
export PRETTIER_PLUGIN="$(rlocation buildbuddy/node_modules/prettier-plugin-organize-imports/index.js)"

cd "$BUILD_WORKSPACE_DIRECTORY"

function timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

function info() {
  echo >&2 -e "\x1b[90m$(timestamp)\x1b[0m\x1b[96m INFO:\x1b[m" "$@" "\x1b[0m"
}

function error() {
  echo >&2 -e "\x1b[90m$(timestamp)\x1b[0m\x1b[91m ERROR:\x1b[m" "$@" "\x1b[0m"
}

LOGDIR=$(mktemp -d)
trap 'rm -r "$LOGDIR"' EXIT
LOGFILES=()

function run() {
  NAME="$1"
  info "Starting $NAME"
  shift
  LOGFILES+=("$NAME")
  {
    "$@" >"$LOGDIR/$NAME" 2>&1 || true
    info "Finished $NAME"
  } &
}

run BuildFiles \
  "$BB_PATH" fix --diff

run GoDeps \
  env GO_PATH="$GO_PATH" \
  tools/fix_go_deps.sh --diff

run GoFormat \
  "$GOFMT_PATH" -d .

run GoImports \
  "$GOIMPORTS_PATH" -d .

run ProtoFormat \
  env CLANG_FORMAT_PATH="$CLANG_FORMAT_PATH" \
  tools/clang-format/clang-format.sh --dry-run

run PrettierFormat \
  "$PRETTIER_PATH" --log-level=warn --check

wait

OK=1
for LOGFILE in "${LOGFILES[@]}"; do
  CONTENT=$(cat "$LOGDIR/$LOGFILE")
  if [[ "$CONTENT" ]]; then
    error "$LOGFILE:"
    echo "$CONTENT"
    OK=0
  fi
done

if ! ((OK)); then
  error "Some checks failed. To fix many common errors, run: ./buildfix.sh -a"
  exit 1
fi

info "All checks passed!"
