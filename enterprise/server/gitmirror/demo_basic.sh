#!/usr/bin/env bash
set -e

# ./demo.sh server: runs the git mirror server
# ./demo.sh client: clones through the git mirror server

mkdir -p /tmp/git-mirror-demo

if [[ "$1" == "server" ]]; then
  trap 'rm -rf /tmp/git-mirror-demo/server' EXIT

  rm -rf /tmp/git-mirror-demo/server
  bazel run -- //enterprise/server/cmd/gitmirror --git.mirror.root_directory="/tmp/git-mirror-demo/server"
  exit
fi

trap 'rm -rf /tmp/git-mirror-demo/client' EXIT

rm -rf /tmp/git-mirror-demo/client
mkdir -p /tmp/git-mirror-demo/client
cd /tmp/git-mirror-demo/client

# Clone buildbuddy from GitHub directly
time git clone https://github.com/buildbuddy-io/buildbuddy
rm -rf ./buildbuddy

sleep 10

# Clone buildbuddy twice through the git mirror (one cold, one warm)
time git clone http://localhost:8180/v1/github.com/buildbuddy-io/buildbuddy
rm -rf ./buildbuddy
time git clone http://localhost:8180/v1/github.com/buildbuddy-io/buildbuddy
