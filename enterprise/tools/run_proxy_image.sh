#!/usr/bin/env bash
set -eu

# Builds and runs the cache proxy (aka "edge") Docker image locally.

DOCKER_ARGS=(
  --rm --interactive --tty
  # Use host networking, for simplicity.
  --net=host
  # Mount a persistent local cache dir. The cache-proxy.local.yaml defines these
  # with a $USER prefix, and the image runs as root, hence the "root_" prefix.
  --volume /tmp/root_filecache:/tmp/root_filecache
  # Even though the cache proxy runs as uid 0, the USER env var is
  # not set for some reason, so set it here.
  --env "USER=root"
  # Mount the local config file.
  --volume "$PWD/enterprise/config/cache-proxy.local.yaml:/cache-proxy.local.yaml"
)

# Running a container_image target just builds and tags it.
# docker run is needed to actually run it.
bazel run //enterprise/server/cmd/cache_proxy:cache_proxy_image
docker run "${DOCKER_ARGS[@]}" bazel/enterprise/server/cmd/cache_proxy:cache_proxy_image \
  --port=8081 \
  --grpc_port=1989 \
  --internal_grpc_port=1990 \
  --monitoring_port=9091 \
  --config_file=/cache-proxy.local.yaml \
  "$@"
