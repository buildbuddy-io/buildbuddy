#!/usr/bin/env bash
# build_abseil.sh - Builds abseil-cpp against BuildBuddy.
# Intended to run as a periodic workflow to generate external traffic.
# Relies on being invoked via `bb run` (sets BUILD_WORKSPACE_DIRECTORY)
# or `bb remote`, which provides RBE config via the repo's .bazelrc.
set -euo pipefail

cd "${BUILD_WORKSPACE_DIRECTORY:-$(pwd)}"

# Query cc_library/cc_binary targets only. Exclude test, benchmark, mock, and
# matcher helpers:
# they pull in optional deps that are not part of the abseil module graph here.
query='kind("cc_(library|binary) rule", @com_google_absl//absl/...) except attr("tags", "manual", @com_google_absl//absl/...) except filter(".*(benchmark|test|mock|matcher).*", @com_google_absl//absl/...)'

config_args=()
if [[ -n "${BAZEL_CONFIG:-}" ]]; then
  IFS=',' read -ra configs <<<"${BAZEL_CONFIG}"
  for config in "${configs[@]}"; do
    [[ -z "${config}" ]] && continue
    config_args+=("--config=${config}")
  done
fi

mapfile -t targets < <(bb query "${config_args[@]}" "$@" "${query}")
if (( ${#targets[@]} == 0 )); then
  echo "No abseil-cpp targets found." >&2
  exit 1
fi

echo "Building ${#targets[@]} abseil-cpp targets..."
bb build "${config_args[@]}" "$@" "${targets[@]}"
