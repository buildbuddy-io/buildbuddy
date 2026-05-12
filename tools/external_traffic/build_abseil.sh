#!/usr/bin/env bash
# build_abseil.sh — Builds abseil-cpp against BuildBuddy RBE.
# Intended to run as a periodic workflow to generate external traffic.
# Relies on being invoked via `bazel run` (sets BUILD_WORKSPACE_DIRECTORY)
# or `bb remote`, which provides RBE config via the repo's .bazelrc.
set -euo pipefail

cd "${BUILD_WORKSPACE_DIRECTORY}"

# Query cc_library/cc_binary targets only — benchmark helpers inside abseil
# carry a dep on @google_benchmark which is not visible from @com_google_absl's
# own module graph, causing analysis errors.
