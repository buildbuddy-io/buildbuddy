#!/usr/bin/env bash

_bazel() {
  bazel --output_base=/tmp/bob/local-rbe "$@"
}

truncate --size=0 /tmp/treecache_build.log
for i in {1..2}; do
  _bazel clean --async
  echo "{t: $(date +%s%N), event: START_BUILD}" >> /tmp/treecache_build.log
  _bazel build //server/util/git/... \
    --config=local-rbe \
    --jobs=32 \
    --noremote_accept_cached
  sleep 5
done
cat /tmp/treecache.log /tmp/treecache_build.log | sort > /tmp/treecache_combined.log
go run analyze_digests.go < /tmp/treecache_combined.log


