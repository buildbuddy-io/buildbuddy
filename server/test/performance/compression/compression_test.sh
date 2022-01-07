#!/bin/bash
set -eu

cd ~/code/bb/buildbuddy

bazel test \
    -c opt \
    //server/test/performance/compression:compression_test \
    --color=yes --curses=no \
    --bes_results_url= \
    --bes_backend= \
    --nocache_test_results \
    --show_progress=0 \
    --test_output=streamed \
    --test_arg=-test.v \
    --test_arg=-test.benchtime=10s \
    --test_arg=-test.bench=. \
    --test_arg=-blob_dir="$HOME/smash-blobs" \
    --test_arg=-remote_cache="grpc://10.142.0.30:1985" \
    "$@"
