#!/usr/bin/env bash
set -e

# Run in VM:
# ./push.sh && gcloud compute ssh NODE --zone=ZONE <debug.sh

# Run in pod:
# ./push.sh && kubectl exec -it -n executor-dev POD -- bash <debug.sh

BRANCH=executor-rcu-dbg

sudo() {
  if [[ $(id -u) == 0 ]]; then
    "$@"
  else
    "$(which sudo)" "$@"
  fi
}

echo "Installing system dependencies..."
export PATH="$PATH:/usr/local/go/bin:$HOME/go/bin"
if ! command -v go; then
  curl -fsSL https://go.dev/dl/go1.22.1.linux-amd64.tar.gz |
    sudo tar -C /usr/local -xzf -
fi
if ! command -v buildozer; then
  go install github.com/bazelbuild/buildtools/buildozer@latest
fi
if ! command -v bazel; then
  curl -fsSL https://github.com/bazelbuild/bazelisk/releases/download/v1.19.0/bazelisk-linux-amd64 | sudo tee /usr/local/bin/bazelisk >/dev/null
  sudo chmod +x /usr/local/bin/bazelisk
  sudo ln /usr/local/bin/bazelisk /usr/local/bin/bazel
fi
INSTALL=()
which gcc || INSTALL+=(gcc)
which g++ || INSTALL+=(g++)
which setfacl || INSTALL+=(acl)
which route || INSTALL+=(net-tools)
which skopeo || INSTALL+=(skopeo)
which umoci || INSTALL+=(umoci)
which jq || INSTALL+=(jq)
which sudo || INSTALL+=(sudo)
which fuser || INSTALL+=(psmisc)
which redis-server || INSTALL+=(redis)
which podman || INSTALL+=(podman)
# which cmake || INSTALL+=(cmake)
# which openssl || INSTALL+=(openssl)
# dpkg -l | grep libssl-dev || INSTALL+=("libssl-dev")
if [[ "${#INSTALL[@]}" != 0 ]]; then
  export DEBIAN_FRONTEND=noninteractive
  sudo apt-get update && sudo apt-get install -y "${INSTALL[@]}"
fi

# Set root dir
if [[ -e /mnt/disks/local/0 ]]; then
  # We're on the host VM
  cd /mnt/disks/local/0
else
  # We're in the executor pod
  cd /buildbuddy
fi

# Clone buildbuddy and run firecracker stress test
mkdir -p debug && cd debug

# Ensure the temp dir is on the same FS as the filecache
mkdir -p tmp
TEMPDIR="$PWD/tmp"

if ! [[ -e buildbuddy ]]; then
  git clone https://github.com/buildbuddy-io/buildbuddy --filter=blob:none
fi
cd buildbuddy
git checkout "$BRANCH"
git fetch origin "$BRANCH"
git reset --hard origin/"$BRANCH"

# If running inside pod, skip running enable_local_firecracker
if [[ $(id -u) != 0 ]]; then
  sudo ./tools/enable_local_firecracker.sh
fi

# Build app and executor
bazel build //enterprise/server
bazel --output_base="$TEMPDIR/executor_output_base" build //enterprise/server/cmd/executor

# Kill existing app/executor
sudo fuser -k 8650/tcp || true
sudo fuser -k 8651/tcp || true

# Run app
bazel run enterprise/server \
  -- \
  --monitoring_port=8650 \
  --telemetry_port=-1 \
  --cache.disk.root_directory="$TEMPDIR/app-cache" &
APP_PID=$!

# Run executor
bazel \
  --output_base="$TEMPDIR/executor_output_base" \
  run \
  --run_under=sudo \
  enterprise/server/cmd/executor \
  -- \
  --monitoring_port=8651 \
  --debug_enable_anonymous_runner_recycling \
  --executor.docker_socket= \
  --executor.enable_podman \
  --executor.root_directory="$TEMPDIR/buildroot" \
  --executor.local_cache_directory="$TEMPDIR/filecache" \
  --executor.local_cache_size_bytes=100000000000 \
  --executor.firecracker_enable_uffd \
  --executor.firecracker_enable_vbd \
  --executor.firecracker_enable_merged_rootfs \
  --executor.enable_local_snapshot_sharing \
  --executor.enable_remote_snapshot_sharing &
EXECUTOR_PID=$!

# Wait for healthy
URLS=(
  "http://localhost:8888/readyz?server-type=prod-buildbuddy-executor"
)
for URL in "${URLS[@]}"; do
  while true; do
    RES=$(curl -fsSL "$URL" || true)
    if [[ "$RES" == OK ]]; then break; fi
    echo "Waiting for ready on $URL: $RES"
    sleep 1
  done
done

echo "=== READY ==="

trap '
  kill $EXECUTOR_PID || true
  kill $APP_PID || true
' EXIT

# Run test build
bazel build enterprise/server/backends/authdb:authdb_test \
  --config=remote \
  --bes_backend= \
  --remote_cache=grpc://localhost:1985 \
  --remote_executor=grpc://localhost:1985 \
  --test_output=errors \
  --jobs=32 \
  --runs_per_test=100
