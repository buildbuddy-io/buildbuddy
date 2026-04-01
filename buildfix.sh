#!/bin/bash

for bazel_cmd in bb bazelisk bazel; do
  if command -v "$bazel_cmd" >/dev/null 2>&1; then
    exec "$bazel_cmd" run -- //tools/lint -fix "$@"
  fi
done

echo "ERROR: couldn't find bb, bazel, or bazelisk in PATH" >&2
exit 1
