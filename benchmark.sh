#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="$1"
RUNS="${2:-10}" 
OUTFILE="${CLUSTER_NAME}.txt"

rm -f "$OUTFILE"

echo "Cluster name is $CLUSTER_NAME"

for i in $(seq 1 $RUNS); do
    echo "Running build $i/$RUNS..."

    # Capture wall time in seconds with high precision
    start=$(date +%s.%N)
    /Users/maggielou/bb/buildbuddy/bazel-bin/cli/cmd/bb/bb_/bb remote --os=linux --arch=amd64 --runner_exec_properties=EstimatedFreeDiskBytes=35GB --runner_exec_properties=Pool=$CLUSTER_NAME --runner_exec_properties=instance_name=$start build //... --remote_cache= --remote_header=x-buildbuddy-api-key=rec2xeVoKl0nU6gad5F7
    end=$(date +%s.%N)

    dur=$(echo "$end - $start" | bc -l)

    # Emit go benchmark format
    printf "Benchmark%s_Run%d\t1\t%.6fs\n" "$CLUSTER_NAME" "$i" "$dur" >> "$OUTFILE"
done

echo "Done. Results saved to $OUTFILE"
