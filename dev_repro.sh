#!/bin/bash
set -euo pipefail

IID=$(uuidgen)
echo "Invocation will be available at https://app.buildbuddy.dev/invocation/$IID?queued=true"

TOPLEVEL_IID=be24c0b8-8742-4cda-b58e-25117ac9dcf8
echo "Invocation ID for cancellation: $TOPLEVEL_IID"

bb --verbose=1 execute \
  --invocation_id="$TOPLEVEL_IID" \
  --remote_header=x-buildbuddy-api-key=$DEV_API_KEY \
  --exec_properties=Pool=workflows \
  --exec_properties=workload-isolation-type=firecracker \
  --exec_properties=container-image=docker://gcr.io/flame-public/rbe-ubuntu20-04-workflows@sha256:271e5e3704d861159c75b8dd6713dbe5a12272ec8ee73d17f89ed7be8026553f \
  --exec_properties=recycle-runner=true \
  --exec_properties=workflow-id=WF0000000000 \
  --exec_properties=EstimatedMemory=10GB \
  --exec_properties=EstimatedFreeDisk=20GB \
  --exec_properties=EstimatedCPU=3000m \
  --exec_properties=init-dockerd=true \
  --action_env=GIT_BRANCH=main \
  --remote_executor=grpcs://remote.buildbuddy.dev \
  -- \
  bash -c '
export HOME=/root
export PATH="/bin:/usr/bin:/usr/sbin:/sbin:/usr/local/bin"

cd /root && mkdir -p workspace && cd workspace

rm -rf ./*

# Use up a bunch of disk
yes | head -c 16000000000 > file.txt

# Use up a bunch of memory and hold onto it for a while
python3 -c "
import time
for _ in range(3):
    s = \"A\" * 8_000_000_000
    time.sleep(1)
"

'
