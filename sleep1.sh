#!/bin/sh
set -eu
start=$(cat /proc/uptime | awk '{print $1}')
podman run --net=none --rm busybox sleep 1
end=$(cat /proc/uptime | awk '{print $1}')
echo $(echo $end - $start | bc) seconds
