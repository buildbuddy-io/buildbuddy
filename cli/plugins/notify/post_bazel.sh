#!/usr/bin/env bash
set -eu

: "${BB_NOTIFY_SOUND=0}"
: "${BB_NOTIFY_SOUND_NAME=Tink}"

# Check for the file written by pre_bazel.
if ! [[ -e "$PLUGIN_TEMPDIR/notify" ]]; then
  exit 0
fi

STATUS_LINE=$(grep "Build" "$1" | grep "complete" | tail -n1 | perl -p -e 's@.*?(Build)@\1@')
ELAPSED_TIME_LINE=$(grep "Elapsed time" "$1" | tail -n1 | perl -p -e 's@.*?(Elapsed)@\1@')

TITLE="Bazel build finished"
MESSAGE="${ELAPSED_TIME_LINE}\n${STATUS_LINE}"

if [[ "$OSTYPE" == darwin* ]]; then
  SCRIPT="display notification \"$MESSAGE\" with title \"$TITLE\""

  if ((BB_NOTIFY_SOUND)); then
    SCRIPT+=" sound name \"$BB_NOTIFY_SOUND_NAME\""
  fi

  osascript -e "$SCRIPT"
  exit 0
fi

notify-send --expire-time 3000 "$TITLE" "$MESSAGE"
