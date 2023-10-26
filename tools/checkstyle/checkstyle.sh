#!/bin/bash
set -euo pipefail

GO_PATH="$(readlink ./go)"
GOFMT_PATH="$(readlink ./gofmt)"
PRETTIER_PATH="$(readlink external/npm/prettier/bin/prettier.sh)"

export RUNFILES_DIR=$(cd ../ && pwd)
export RUNFILES="${RUNFILES_DIR}_manifest"

cd "$BUILD_WORKSPACE_DIRECTORY"

function info() {
  echo >&2 -e "\x1b[90m$(date)\x1b[0m\x1b[96m INFO:\x1b[m" "$@" "\x1b[0m"
}

function error() {
  echo >&2 -e "\x1b[90m$(date)\x1b[0m\x1b[91m ERROR:\x1b[m" "$@" "\x1b[0m"
}

info "Running gofmt"
GOFMT_DIFF=$("$GOFMT_PATH" -d . | tee /dev/stderr || true)

info "Running tools/fix_go_deps.sh --diff (go.mod, go.sum, deps.bzl)"
GO_DEPS_DIFF=$(GO_PATH="$GO_PATH" tools/fix_go_deps.sh --diff | tee /dev/stderr || true)

info "Running prettier --check (js, jsx, ts, tsx, html, css, yaml, json, md, xml)"
PRETTIER_DIFF=$(PRETTIER_PATH="$PRETTIER_PATH" tools/prettier/prettier.sh --loglevel=warn --check 2>&1 | tee /dev/stderr || true)

if [[ "$GO_DEPS_DIFF" ]] || [[ "$GOFMT_DIFF" ]] || [[ "$PRETTIER_DIFF" ]]; then
  error "Some checks failed."
  exit 1
fi

info "All checks passed!"
