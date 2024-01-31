#!/usr/bin/env bash

SCRIPT="
cd /tmp

export CPU=\$(lscpu | grep 'Vendor ID' | awk '{print \$3}' | perl -pe 's@.*(AMD|Intel).*@\1@')

function measure() {
  export NAME=\"\$1\"
  shift
  python3 -c '
import os
import subprocess
import sys
import time
start = time.time()
subprocess.run(sys.argv[1:])
end = time.time()
print(\"%s\t%s\t%s\t%s\t%.3fs\t%7.1f MB/s\" % (os.environ[\"ENV\"], os.environ[\"CPU\"], os.environ[\"MACHINE\"], os.environ[\"NAME\"], end-start, 1024/(end-start)))
' \"\$@\" 1>&2
}

rm random.img zeros.img zeros_copy.img &>/dev/null || true
measure WriteRand1GB sh -c 'dd if=/dev/urandom of=random.img bs=1M count=1024 1>/dev/null 2>/dev/null'
measure WriteZero1GB sh -c 'dd if=/dev/zero of=zeros.img bs=1M count=1024 1>/dev/null 2>/dev/null'
measure CopyFile1GB  cp zeros.img zeros_copy.img
measure ReadFile1GB  sh -c 'cat zeros.img >/dev/null'
"

MACHINE=Host bash -c "$SCRIPT"

bb execute \
  --action_env=MACHINE=MicroVM \
  --action_env=ENV="$ENV" \
  --remote_executor=grpc://localhost:1985 \
  --exec_properties=workload-isolation-type=firecracker \
  --exec_properties=EstimatedFreeDiskBytes=5GB \
  --remote_instance_name=$(date +%s) \
  "$@" \
  -- bash -c "$SCRIPT"

