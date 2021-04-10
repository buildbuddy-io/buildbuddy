#!/bin/bash
set -e

cd "$(dirname "$0")"

wait_executor() {
  while [[ $(curl -s "http://localhost:8888/healthz?server-type=prod-buildbuddy-executor") != "OK" ]]; do
    sleep 0.1
  done
}

rand_01() {
  python -c 'import random; random.random() < 0.5 and exit(1)'
}

run() {
  local args=("$@")
  (
    set -e

    sudo fuser -k 1987/tcp || true

    (
      cd ~/code/bb/buildbuddy-internal
      bazel run //enterprise/server/cmd/executor -- --app.log_level=error "${args[@]}" | tee /tmp/executor.log
    ) &
    executor_pid=$!

    cd ~/code/bb/buildbuddy
    bazel clean
    wait_executor
    bazel build //server \
        --config=local-remote --remote_instance_name="$(date +%s)" \
        --remote_header=x-buildbuddy-api-key=XrB5MdZmngWlIYu0j2Pa 2>&1 |
        tee /tmp/exp.log
    local t
    t=$(grep -P 'Elapsed time: .*?, Critical Path:' /tmp/exp.log | perl -pe 's/.*?Elapsed time: ([^,]+).*/\1/')

    echo "${args[@]}" "$t" > /tmp/exp_result.log

    kill -INT "$executor_pid"
    sleep 2 # wait for all servers to be killed
  )
}

run_a() {
  run --force_recycle=true
}

run_b() {
  run --force_recycle=false
}

rm /tmp/exp_results.log
while true; do
  if rand_01; then
    run_a
  else
    run_b
  fi
  cat /tmp/exp_result.log >> /tmp/exp_results.log
done
