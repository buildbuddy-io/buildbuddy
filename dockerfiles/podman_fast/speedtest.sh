#!/usr/bin/env bash
set -e

: "${MACHINE_IMAGE:=podman-fedora}"
# Executor image
# : "${MACHINE_IMAGE=gcr.io/flame-public/buildbuddy-executor-enterprise:enterprise-v2.30.0}"
: "${TEST_IMAGE=ubuntu:22.04}"

cd "$(dirname "$0")"

make all

podman run --rm -it --transient-store --privileged --user=0 "$MACHINE_IMAGE" sh -c "
  podman version
  podman pull $TEST_IMAGE

  for _ in \$(seq 1 25); do
    time podman run --rm --runtime=crun --transient-store --net=none --log-driver=none  \"$TEST_IMAGE\"
  done
"
