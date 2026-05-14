#!/usr/bin/env bash

set -euo pipefail

# Verify that the OSS server has no enterprise dependencies.
enterprise_deps="$(cat "$1")"
if [ -n "$enterprise_deps" ]; then
  echo "The OSS server has enterprise dependencies:"
  echo "$enterprise_deps"
  exit 1
fi
