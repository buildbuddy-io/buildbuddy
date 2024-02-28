#!/usr/bin/env bash
set -e

# This script wraps GitHub's runner, starting it with the one-time-use config
# provided as the environment variable $RUNNER_ENCODED_JITCONFIG. It also
# handles killing the runner if it does not accept a job within the configured
# idle timeout, passed as $RUNNER_IDLE_TIMEOUT. This prevents runner tasks from
# waiting around forever in case the number of runners exceeds the number of
# available jobs, for example if a workflow job is canceled.

# TODO(bduffany): rewrite this in Go

# Turn on job control so that each background job starts in its own process
# group, so we can clean up these jobs more easily.
set -m

cd /actions-runner

# When this script exits, clean up all background jobs.
trap '
  jobs -p | while read -r PID; do
    echo >&2 "Cleaning up process group $PID"
    kill -TERM -- -$PID || true
  done
' EXIT

# Start the runner and redirect its output to a log file.
RUNNER_LOG=/tmp/runner.log
truncate --size=0 "$RUNNER_LOG"
(
  ./run.sh --jitconfig "$RUNNER_ENCODED_JITCONFIG" 2>&1 |
    tee "$RUNNER_LOG" >&2
  echo >&2 'Runner exited.'
) &
RUNNER_PID=$!

# Kill the runner if it idles for too long.
(
  if ! timeout "$RUNNER_IDLE_TIMEOUT" sh -c "
    tail -n+1 --follow \"$RUNNER_LOG\" 2>/dev/null | grep -q -m1 'Running job:'
  "; then
    echo >&2 "Runner did not pick up job within ${RUNNER_IDLE_TIMEOUT}s; killing."
    kill -KILL -- -$RUNNER_PID
  fi
) &

wait "$RUNNER_PID"
echo >&2 "Finished waiting for runner."
