#!/usr/bin/env bash
# build_abseil.sh — Builds abseil-cpp against BuildBuddy RBE (dev).
# Intended to run as a periodic workflow to generate external traffic.
set -euo pipefail

# Locate the abseil_workspace via Bazel runfiles.
# When run with `bazel run`, RUNFILES_DIR or the sibling-manifest is set.
if [[ -n "${RUNFILES_DIR:-}" ]]; then
  WORKSPACE_DIR="${RUNFILES_DIR}/_main/tools/external_traffic/abseil_workspace"
elif [[ -f "${BASH_SOURCE[0]}.runfiles_manifest" ]]; then
  WORKSPACE_DIR="$(grep '_main/tools/external_traffic/abseil_workspace/MODULE.bazel' \
      "${BASH_SOURCE[0]}.runfiles_manifest" | head -1 | awk '{print $2}' | xargs dirname)"
else
  # Fallback: source-tree relative path (for direct script invocation).
  WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/abseil_workspace"
fi

BB_GRPC="${BB_GRPC_ENDPOINT:-buildbuddy.buildbuddy.dev}"
BB_APP="${BB_APP_ENDPOINT:-buildbuddy.buildbuddy.dev}"
# BB_API_KEY can be set explicitly; fall back to BUILDBUDDY_API_KEY which is
# injected automatically by the BuildBuddy workflow runner.
BB_API_KEY="${BB_API_KEY:-${BUILDBUDDY_API_KEY:-}}"

# Use bazel from PATH (workflows supply bazelisk as bazel).
BAZEL="${BAZEL_BINARY:-bazel}"

echo "================================================"
echo "build_abseil: building @abseil-cpp//... via RBE"
echo "  workspace : ${WORKSPACE_DIR}"
echo "  endpoint  : grpcs://${BB_GRPC}"
echo "================================================"

# Copy the workspace to a writable temp dir so bazel can write its output base.
TMP_WS="$(mktemp -d /tmp/build_abseil_ws_XXXXXX)"
trap 'rm -rf "${TMP_WS}"' EXIT
cp -r "${WORKSPACE_DIR}/"* "${TMP_WS}/"
cd "${TMP_WS}"

# Write a transient .bazelrc so we don't commit credentials.
BAZELRC="$(mktemp /tmp/build_abseil_XXXXXX.bazelrc)"
trap 'rm -f "${BAZELRC}"; rm -rf "${TMP_WS}"' EXIT

cat > "${BAZELRC}" <<EOF
common:rbe --enable_bzlmod
common:rbe --noenable_workspace
common:rbe --remote_executor=grpcs://${BB_GRPC}
common:rbe --bes_backend=grpcs://${BB_GRPC}
common:rbe --bes_results_url=https://${BB_APP}/invocation/
common:rbe --remote_timeout=10m
common:rbe --jobs=100
common:rbe --verbose_failures
common:rbe --build_metadata=TAGS=external-traffic,abseil
common:rbe --extra_toolchains=@toolchains_buildbuddy//toolchains/cc:ubuntu_gcc_x86_64
common:rbe --platforms=@toolchains_buildbuddy//platforms:linux_x86_64
common:rbe --extra_execution_platforms=@toolchains_buildbuddy//platforms:linux_x86_64
EOF

if [[ -n "${BB_API_KEY}" ]]; then
  echo "common:rbe --remote_header=x-buildbuddy-api-key=${BB_API_KEY}" >> "${BAZELRC}"
fi

# Build cc_library and cc_binary targets only, excluding tests and benchmarks.
# Benchmarks are tagged "benchmark" in abseil's BUILD files.
# Test targets are rule-kind cc_test (excluded by not being cc_library/cc_binary).
# Some cc_library helpers for benchmarks include "benchmark" in their name.
echo "Querying buildable (non-test, non-benchmark) targets in @abseil-cpp..."
BUILD_TARGETS="$(${BAZEL} --bazelrc="${BAZELRC}" \
  query 'kind("cc_(library|binary)", @abseil-cpp//...) except filter("(benchmark|_bench)", @abseil-cpp//...)' \
  --config=rbe 2>/dev/null)"
TARGET_COUNT="$(echo "${BUILD_TARGETS}" | grep -c '.' || true)"
echo "Building ${TARGET_COUNT} targets..."
# shellcheck disable=SC2086
${BAZEL} --bazelrc="${BAZELRC}" build ${BUILD_TARGETS} --config=rbe
