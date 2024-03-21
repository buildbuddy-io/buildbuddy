#!/usr/bin/env bash

# COW=1 ./executor.sh to enable copy-on-write
: "${COW:=0}"

# CLEAN=0 ./executor.sh to preserve state across runs
: "${CLEAN:=1}"

COWFLAGS=()
if ((COW)); then
  COWFLAGS=(
    --executor.firecracker_enable_uffd
    --executor.firecracker_enable_vbd
    --executor.firecracker_enable_merged_rootfs
    --executor.firecracker_enable_local_snapshot_sharing
    --executor.firecracker_enable_remote_snapshot_sharing
  )
fi
if ((CLEAN)); then
  # Clear out queued executions
  redis-cli FLUSHALL
  # Clear filecache and runner pool state
  sudo rm -rf /tmp/root_filecache /tmp/root_remote_build/_runner_pool_state.bin
fi

sudo ./tools/enable_local_firecracker.sh
bazel run \
  --run_under=sudo \
  enterprise/server/cmd/executor \
  -- \
  "${COWFLAGS[@]}" \
  --executor.docker_socket= \
  --executor.enable_firecracker \
  --executor.firecracker_debug_stream_vm_logs \
  --debug_enable_anonymous_runner_recycling

