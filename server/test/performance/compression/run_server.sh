#!/bin/bash
set -eu

cd ~/code/bb/buildbuddy

git checkout zstd-klauspost
git fetch origin zstd-klauspost
git reset --hard origin/zstd-klauspost

rm -rf /tmp/buildbuddy-enterprise-cache

bazel run -c opt //enterprise/server -- \
  --cache.zstd_transcoding_enabled=true \
  "$@"
