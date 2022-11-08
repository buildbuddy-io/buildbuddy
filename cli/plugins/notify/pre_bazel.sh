#!/usr/bin/env bash
if ! which python3 &>/dev/null; then
  echo -e "\x1b[33mWarning: notify plugin is disabled: missing 'python3' in \$PATH\x1b[m" >&2
  exit 0
fi
if [[ "$OSTYPE" == darwin* ]] && ! which osascript &>/dev/null; then
  echo -e "\x1b[33mWarning: notify plugin is disabled: missing 'osascript' in \$PATH\x1b[m" >&2
  exit 0
elif [[ "$OSTYPE" == linux* ]] && ! which notify-send &>/dev/null; then
  echo -e "\x1b[33mWarning: notify plugin is disabled: missing 'notify-send' in \$PATH\x1b[m" >&2
  exit 0
fi

exec python3 ./pre_bazel.py "$@"
