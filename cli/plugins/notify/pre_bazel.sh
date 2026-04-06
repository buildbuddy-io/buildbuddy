#!/usr/bin/env bash
if ! which python3 &>/dev/null; then
  exit 0
fi
if [[ "$OSTYPE" == darwin* ]] && ! which osascript &>/dev/null; then
  exit 0
elif [[ "$OSTYPE" == linux* ]] && ! which notify-send &>/dev/null; then
  exit 0
fi

exec python3 ./pre_bazel.py "$@"
