#!/bin/bash
set -eu

if [[ "$(id -u)" != 0 ]]; then
  echo >&2 "$0: error: must be run as root"
  exit 1
fi

echo >&2 "Cleaning up any running firecracker processes"
killall -KILL firecracker || true

echo >&2 "Cleaning up network namespaces"
ip netns list | awk '{ print $1 }' | while read -r netns; do
  [[ "$netns" == bb-executor-* ]] && (
    set -x
    ip netns delete "$netns"
  )
done || true
