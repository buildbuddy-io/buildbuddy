#!/usr/bin/env bash
set -e

for ((i = 0; i < 100; i++)); do
	echo "BAZEL-DIFF ITERATION $i"
	/tmp/bazel_diff generate-hashes -w $PWD
	git checkout HEAD~1
done
