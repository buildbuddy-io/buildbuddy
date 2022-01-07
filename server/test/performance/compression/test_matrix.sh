#!/bin/bash
set -e

cd ~/code/bb/buildbuddy

./server/test/performance/compression/sync.sh

: "${METHODS:=Write Read}"
: "${SIZES:=smol big}"
: "${LIBRARIES:=klauspost datadog valyala}"

for method in $METHODS; do
    for size in $SIZES; do
        for lib in $LIBRARIES; do
            echo "method=$method"
            echo "size=$size"
            echo "lib=$lib"

            ./server/test/performance/compression/compression_test.sh \
                --test_arg=-test.bench="$method" \
                --test_arg=-bs_blob_size="$size" \
                --test_arg=-zstd_lib="$lib"

        done
    done
done
