#!/usr/bin/env bash

set -euo pipefail

binary="$1"
max_bytes="$2"
actual_bytes="$(wc -c < "$binary")"

if (( actual_bytes > max_bytes )); then
  echo "goinit is too large: ${actual_bytes} bytes (limit: ${max_bytes})" >&2
  echo "Check for new transitive dependencies or embedded data before raising the limit." >&2
  exit 1
fi

echo "goinit size: ${actual_bytes} bytes (limit: ${max_bytes})"
