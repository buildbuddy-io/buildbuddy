#!/usr/bin/env bash
if ! which python3 &>/dev/null; then
  echo -e "\x1b[33mWarning: open-invocation plugin is disabled: missing 'python3' in \$PATH\x1b[m" >&2
  exit 0
fi
open_command=$( (which xdg-open open | head -n1) || true)
if [[ ! "$open_command" ]]; then
  echo -e "\x1b[33mWarning: open-invocation plugin is disabled: missing 'open' or 'xdg-open' in \$PATH\x1b[m" >&2
  exit
fi

exec python3 ./pre_bazel.py "$@"
