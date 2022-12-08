#!/usr/bin/env bash
if ! which python3 &>/dev/null; then
  echo -e "\x1b[33mWarning: go-highlight plugin is disabled: missing 'python3' in \$PATH\x1b[m" >&2
  exec cat
fi
exec python3 ./handle_bazel_output.py "$@"
