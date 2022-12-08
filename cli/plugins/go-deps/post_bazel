#!/usr/bin/env bash
if ! which python3 &>/dev/null; then
  echo -e "\x1b[33mWarning: go-deps plugin is disabled: missing 'python3' in \$PATH\x1b[m" >&2
  exit 0
fi
exec python3 ./post_bazel.py "$@"
