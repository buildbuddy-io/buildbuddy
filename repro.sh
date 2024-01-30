#!/usr/bin/env bash

for _ in {1..100}; do
bb --verbose=1 execute \
  --remote_executor=grpc://localhost:1985 \
  --exec_properties=workload-isolation-type=firecracker \
  --exec_properties=init-dockerd=true \
  --exec_properties=recycle-runner=true \
  --exec_properties=EstimatedCPU='4' \
  --exec_properties=EstimatedMemory='4.5GB' \
  -- bash -c "
run_number=\$(cat /tmp/run.txt || printf 1)
echo Run number \$run_number
printf '%d' \$(( run_number + 1 )) > /tmp/run.txt

docker pull mysql:8.0
docker run -d --rm --name=db mysql:8.0
while ! docker inspect db &>/dev/null ; do
  echo 'Waiting for container...'
  sleep 2
done
# TODO: try connecting to mysql
echo 'Killing container...'
docker rm -f db
"
done
