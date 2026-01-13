#!/bin/bash
# Wrapper script for use with bazel run --run_under
#
# Usage:
#   bazel run --run_under=$(pwd)/tools/run_logger/wrapper.sh //my:target

# Find the run_logger binary relative to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_LOGGER="${SCRIPT_DIR}/run_logger"

# If run_logger doesn't exist at that location, try to find it in runfiles
if [[ ! -x "$RUN_LOGGER" ]]; then
    # When executed through bazel run, binaries are in runfiles
    RUN_LOGGER="${SCRIPT_DIR}/../../bazel-out/../../../bazel-bin/tools/run_logger/run_logger_/run_logger"
fi

# Execute the run_logger with all arguments passed through
exec "$RUN_LOGGER" "$@"
