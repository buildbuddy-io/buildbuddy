#!/usr/bin/env bash

bazel run enterprise/tools/replay_action -- \
  --action_digest=/blobs/blake3/5e83daf30d10b458202c8443276bcef0569dc417b0e1436169d09720eb87223a/384 \
  --source_executor=grpcs://remote.buildbuddy.io \
  --source_api_key=$BB_API_KEY \
  --target_executor=grpc://localhost:1985
