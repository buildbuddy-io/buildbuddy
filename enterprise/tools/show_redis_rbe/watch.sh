#!/usr/bin/env bash
set -e
bazel build enterprise/tools/show_redis_rbe
cp bazel-bin/enterprise/tools/show_redis_rbe/show_redis_rbe_/show_redis_rbe /tmp/redis-rbe
trap 'rm -f /tmp/redis-rbe' EXIT

COMMAND=watch
INTERVAL=1
if [[ $(command -v hwatch) ]]; then
  COMMAND=hwatch
  INTERVAL=0.1
fi

"$COMMAND" -n "$INTERVAL" /tmp/redis-rbe
