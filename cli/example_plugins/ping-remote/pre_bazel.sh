#!/usr/bin/env bash

# Look for the effective (last) "--remote_executor" flag value,
# converting "--remote_executor\n<value>" to "--remote_executor=<value>".
REMOTE_EXECUTOR=$(
  perl -p -e 's@^--remote_executor\n@--remote_executor=@' "$1" |
    grep -E '^--remote_executor=' |
    perl -p -e 's@^--remote_executor=(grpcs?://)?(.*?)(:\d+)?$@\2@' |
    tail -n 1
)
if ! [[ "$REMOTE_EXECUTOR" ]]; then
  # Remote execution is not enabled; do nothing.
  exit 0
fi

function timeout() {
  perl -e 'alarm shift; exec @ARGV' "$@"
}

# Make sure we can ping the remote execution service in 500ms.
if ! timeout 0.5 ping -c 1 "$REMOTE_EXECUTOR" &>/dev/null; then
  # Network is spotty; disable remote execution.
  echo "--remote_executor=" >>"$1"
  echo >&2 -e "\x1b[33mWarning: failed to reach $REMOTE_EXECUTOR. Disabling remote execution.\x1b[m"
fi
