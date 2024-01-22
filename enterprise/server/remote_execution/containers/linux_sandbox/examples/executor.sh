#!/usr/bin/env bash
set -eu

bb-executor --podman -- --executor.enable_linux_sandbox
