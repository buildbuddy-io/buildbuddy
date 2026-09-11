#!/usr/bin/env bash
set -euo pipefail
[[ "$2" == "${EXPECTED_ACTION:-.artifacts_exist}" ]]
echo "$1" >> "$RELEASE_LOG"
case "$1" in
  after) exit "${AFTER_STATUS:-0}" ;;
  run) exit "${RUN_STATUS:-0}" ;;
  *) exit 99 ;;
esac
