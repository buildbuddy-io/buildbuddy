#!/usr/bin/env bash
# BB CLI Plugin: Automatically add run_logger to `bazel run` commands
#
# This plugin intercepts `bazel run` commands and adds --run_under
# to stream output to BuildBuddy.

set -e

ARGS_FILE="$1"

# Read the command from args file (first non-flag argument is the command)
COMMAND=$(grep -v '^--' "$ARGS_FILE" | head -n 1 || echo "")

# Only apply to "run" commands
if [[ "$COMMAND" != "run" ]]; then
  exit 0
fi

# Check if --run_under is already set
if grep -q '^--run_under' "$ARGS_FILE"; then
  # User already specified --run_under, don't override
  exit 0
fi

# Find the workspace root to locate run_logger binary
WORKSPACE_ROOT="${BUILD_WORKSPACE_DIRECTORY:-$(bazel info workspace 2>/dev/null || pwd)}"

# Path to the run_logger binary (after it's been built)
RUN_LOGGER_PATH="${WORKSPACE_ROOT}/bazel-bin/tools/run_logger/run_logger_/run_logger"

# Check if run_logger exists
if [[ ! -x "$RUN_LOGGER_PATH" ]]; then
  # Try to build it on the fly
  echo >&2 "Building run_logger..."
  if bazel build //tools/run_logger:run_logger 2>/dev/null; then
    echo >&2 "✓ run_logger built successfully"
  else
    echo >&2 "Warning: run_logger not found at $RUN_LOGGER_PATH"
    echo >&2 "Run: bazel build //tools/run_logger:run_logger"
    exit 0
  fi
fi

# Add --run_under flag to the args file
echo "--run_under=$RUN_LOGGER_PATH" >> "$ARGS_FILE"

echo >&2 "✓ Added run_logger to bazel run command"
