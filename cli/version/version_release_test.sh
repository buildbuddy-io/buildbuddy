#!/usr/bin/env bash

set -euo pipefail

actual_version="$("$1")"
expected_version="$(cat "$2")"

if [[ "$actual_version" != "$expected_version" ]]; then
  echo "expected CLI version $expected_version, got: $actual_version" >&2
  exit 1
fi
