#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  after) exit "${AFTER_STATUS:-0}" ;;
  run) exit "${RUN_STATUS:-0}" ;;
  *) echo >&2 "Unexpected component: $1"; exit 1 ;;
esac
