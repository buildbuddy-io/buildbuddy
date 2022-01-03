#!/bin/bash

index="${1?}"

cache_dir_suffix=$((1 + index))
cache_port=$((1337 + index))
telemetry_port=$((9099 + index))
monitoring_port=$((9090 + index))
grpc_port=$((1985 + index))

ENABLE_COMPRESSION=true

compression_args=()
if [[ "$ENABLE_COMPRESSION" == true ]]; then
  compression_args=(
    --cache.zstd_storage_enabled=true
    --cache.zstd_capability_enabled=true
  )
fi

exec bazel run enterprise/server -- \
  --config_file=enterprise/config/buildbuddy.local.yaml \
  --cache.disk.root_directory="/tmp/cache$cache_dir_suffix" \
  --cache.max_size_bytes=1000000000 \
  --cache.distributed_cache.redis_target="localhost:6379" \
  --cache.distributed_cache.listen_addr="localhost:$cache_port" \
  --cache.distributed_cache.replication_factor=3 \
  --cache.distributed_cache.cluster_size=4 \
  --grpc_port="$grpc_port" \
  --telemetry_port="$telemetry_port" \
  --monitoring_port="$monitoring_port" \
  "${compression_args[@]}" \
  "$@"
