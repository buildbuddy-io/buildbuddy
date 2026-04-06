#!/usr/bin/env bash
if ! which python3 &>/dev/null; then
  exit 0
fi
open_command=$( (which xdg-open open | head -n1) || true)
if [[ ! "$open_command" ]]; then
  exit 0
fi

exec python3 ./pre_bazel.py "$@"
