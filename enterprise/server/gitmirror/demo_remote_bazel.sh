#!/usr/bin/env bash
set -e

mkdir -p /tmp/git-mirror-demo

# Get machine's assigned IPv4 address on local network,
# so we can reach the proxy from within RBE containers
LOCAL_IP=$(ip route | grep default | perl -pe 's@.*src (.*?) .*@\1@')

if [[ "$1" == "app" ]]; then
  exec bazel run -- //enterprise/server \
      --app.events_api_url="grpc://$LOCAL_IP:1985" \
      --app.cache_api_url="grpc://$LOCAL_IP:1985" \
      --app.remote_execution_api_url="grpc://$LOCAL_IP:1985"
fi

if [[ "$1" == "executor" ]]; then
  # Defaults to docker - this is fine for demo purposes
  exec bazel run -- //enterprise/executor
fi

if [[ "$1" == "proxy" ]]; then
  # trap 'rm -rf /tmp/git-mirror-demo/git' EXIT
  # rm -rf /tmp/git-mirror-demo/git

  bazel run -- //enterprise/server/cmd/gitmirror \
    --git.mirror.root_directory="/tmp/git-mirror-demo/git"
fi

if [[ "$1" == "remote-bazel" ]]; then
  : "${BUILDBUDDY_API_KEY?}"
  PROXY_FLAG=""
  if [[ "${USE_PROXY:-}" == 1 ]]; then
    PROXY_FLAG="--runner_extra_flags=--git_proxy=http://$LOCAL_IP:8180/v1/"
  fi
  # Set git proxy runner flags manually (experiment setup is a hassle)
  bazel build //cli

  : "${N:=5}"
  for _ in $(seq 1 "$N"); do
    ./bazel-bin/cli/cmd/bb/bb_/bb \
      remote \
      $PROXY_FLAG \
      --runner_extra_flags="--skip_bazel_workspace_lock_check" \
      --runner_exec_properties="workload-isolation-type=docker" \
      --runner_exec_properties=EstimatedComputeUnits=1 \
      --runner_exec_properties=EstimatedMemory=1GB \
      --runner_exec_properties=EstimatedCPU=1 \
      --runner_exec_properties=salt="$(date +%s)" \
      --remote_runner=grpc://localhost:1985 \
      --script='echo Done'
  done
  wait

  exit
fi

echo >&2 "usage: app|executor|proxy|remote-bazel"
exit 1
