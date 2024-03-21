#!/usr/bin/env bash

bazel run enterprise/tools/replay_action -- \
  --action_digest=/blobs/blake3/49dd6ace2e65f5f4107719934eb9b6adef6e73d256bbbd15b7afecbf7f136df2/639 \
  --source_executor=grpc://localhost:1985 \
  --target_executor=grpc://localhost:1985 \
  "$@"

# To replay with podman:
# --target_headers=x-buildbuddy-platform.workload-isolation-type=podman --target_headers=x-buildbuddy-platform.dockerNetwork=bridge
