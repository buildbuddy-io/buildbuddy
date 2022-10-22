#!/usr/bin/env bash
if ! which python3 &>/dev/null; then
  echo -e "\x1b[33mWarning: open_invocation plugin is disabled: missing 'python3' in \$PATH\x1b[m" >&2
  exit 0
fi
if ! which xdg-open open &>/dev/null; then
  echo -e "\x1b[33mWarning: open_invocation plugin is disabled: missing 'open' or 'xdg-open' in \$PATH\x1b[m" >&2
  exit
fi

exec python3 ./pre_bazel.py "$@"
