#!/usr/bin/env bash

# Builds and runs the executor Docker image locally.

DOCKER_ARGS=(
  # Make sure we can access /dev/kvm (for firecracker).
  --privileged
  # Mount persistent build root dirs and filecache so that
  # firecracker image warmup doesn't take forever.
  # The executor.local.yaml defines these with a $USER prefix,
  # and the executor image will run as root, hence the "root_" prefix.
  --volume /tmp/root_remote_build:/tmp/root_remote_build
  --volume /tmp/root_filecache:/tmp/root_filecache
  # Even though the executor runs as uid 0, the USER env var is
  # not set for some reason, so set it here.
  --env "USER=root"
  # Mount gcloud config so we can pull from gcr.io
  --volume "$HOME/.config/gcloud:/root/.config/gcloud"
  # Mount the docker socket.
  --volume /var/run/docker.sock:/var/run/docker.sock
  # Mount the local config file.
  --volume "$PWD/enterprise/config/executor.local.yaml:/executor.local.yaml"
  # Note: we need "--net=host" but don't need to set it explicitly,
  # since the `bazel run` target created by rules_docker sets it by default.
)

bazel run //enterprise/server/cmd/executor:executor_image -- "${DOCKER_ARGS[@]}" -- \
  --monitoring_port=9091 \
  --config_file=/executor.local.yaml \
  "$@"
