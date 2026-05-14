#!/usr/bin/env bash

set -euo pipefail

# Verify that the CLI has no enterprise dependencies.
enterprise_deps="$(cat "$1")"
if [ -n "$enterprise_deps" ]; then
  echo "The CLI has enterprise dependencies:"
  echo "$enterprise_deps"
  exit 1
fi
