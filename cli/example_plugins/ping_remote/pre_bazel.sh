#!/usr/bin/env bash

if ! grep -E '^--remote_executor=.*\.buildbuddy\.io$' "$1" &>/dev/null; then
  # BB remote execution is not enabled; do nothing.
  exit 0
fi

# Make sure we can ping the remote execution service in 500ms.
if ! timeout 0.5 ping -c 1 remote.buildbuddy.io &>/dev/null; then
  # Network is spotty; disable remote execution.
  echo "--remote_executor=" >>"$1"
fi
