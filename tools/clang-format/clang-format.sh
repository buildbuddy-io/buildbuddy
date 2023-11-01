#!/usr/bin/env bash
set -euo pipefail

: "${CLANG_FORMAT_PATH:=$(command -v clang-format)}"

if ! [[ "$CLANG_FORMAT_PATH" ]]; then
  echo >&2 "Missing clang-format in \$PATH"
  exit 1
fi

git ls-files | grep '\.proto' | xargs "$CLANG_FORMAT_PATH" --style=google "$@"
