#!/bin/bash

# From https://github.com/firecracker-microvm/firecracker/blob/main/docs/getting-started.md#running-firecracker

ARCH="$(uname -m)"
API_SOCKET="./firecracker.socket"

# Remove API unix socket
rm -f $API_SOCKET

# Start docker
sudo systemctl start docker

# Build and run firecracker
sudo ./firecracker/tools/devtool build
./firecracker/build/cargo_target/${ARCH}-unknown-linux-musl/debug/firecracker \
    --api-sock "${API_SOCKET}"