#!/usr/bin/env bash

# Runs the Generate Kythe Annotations workflow end-to-end in a scratch
# workspace using the real Kythe tarball. Intended to be invoked via
# rules_bazel_integration_test.

set -euo pipefail

bazel="${BIT_BAZEL_BINARY:-}"
workspace_dir="${BIT_WORKSPACE_DIR:-}"
kythe_url="${KYTHE_TARBALL_URL:-https://storage.googleapis.com/buildbuddy-tools/archives/kythe-v0.0.76-buildbuddy.tar.gz}"
kythe_sha256="${KYTHE_TARBALL_SHA256:-}"

if [[ -z "${bazel}" ]]; then
  echo >&2 "ERROR: BIT_BAZEL_BINARY not set"
  exit 1
fi
if [[ -z "${workspace_dir}" ]]; then
  echo >&2 "ERROR: BIT_WORKSPACE_DIR not set"
  exit 1
fi

echo "=================================================="
echo "Kythe workflow integration test"
echo "=================================================="
echo "Bazel binary: ${bazel}"
echo "Workspace: ${workspace_dir}"
echo "Kythe URL: ${kythe_url}"
echo "Kythe SHA256 (optional): ${kythe_sha256}"
echo "=================================================="

cd "${workspace_dir}"
export BUILDBUDDY_CI_RUNNER_ROOT_DIR="${workspace_dir}/.ci_runner_root"
mkdir -p "${BUILDBUDDY_CI_RUNNER_ROOT_DIR}"

kythe_dir="${BUILDBUDDY_CI_RUNNER_ROOT_DIR}/kythe"
mkdir -p "${kythe_dir}"

tmp_tar="${workspace_dir}/kythe.tar.gz"
echo "Downloading Kythe tarball..."
curl -L "${kythe_url}" -o "${tmp_tar}"

if [[ -n "${kythe_sha256}" ]]; then
  echo "${kythe_sha256}  ${tmp_tar}" | shasum -a 256 -c -
fi

echo "Extracting Kythe..."
tar -xzf "${tmp_tar}" -C "${kythe_dir}" --strip-components 1
rm -f "${tmp_tar}"

export KYTHE_DIR="${kythe_dir}"
export BUILDBUDDY_ARTIFACTS_DIRECTORY="${workspace_dir}/artifacts"
mkdir -p "${BUILDBUDDY_ARTIFACTS_DIRECTORY}"

# For Bazel 8+, bzlmod is enabled by default; inject_repository works.
KYTHE_ARGS="--inject_repository=kythe_release=${KYTHE_DIR}"
KYTHE_ARGS+=" --experimental_extra_action_top_level_only=false"
KYTHE_ARGS+=" --experimental_extra_action_filter='^//'"

echo "Running Kythe build..."
set -x
"${bazel}" --bazelrc="${KYTHE_DIR}/extractors.bazelrc" build ${KYTHE_ARGS} //:hello
set +x

echo "Indexing Kythe outputs..."
set -x
find -L bazel-out -name "*.java.kzip" | xargs -P "$(nproc)" -n 1 java -jar "${KYTHE_DIR}/indexers/java_indexer.jar" | "${KYTHE_DIR}/tools/dedup_stream" >> kythe_entries

"${KYTHE_DIR}"/tools/write_tables --entries kythe_entries --out leveldb:kythe_tables
"${KYTHE_DIR}"/tools/export_sstable --input leveldb:kythe_tables --output="${BUILDBUDDY_ARTIFACTS_DIRECTORY}/kythe_serving.sst"
set +x

echo "Asserting Kythe artifacts..."
test -s kythe_entries
test -d kythe_tables || test -f kythe_tables  # leveldb directory or file
test -s "${BUILDBUDDY_ARTIFACTS_DIRECTORY}/kythe_serving.sst"

echo "=================================================="
echo "Kythe workflow integration test PASSED"
echo "Artifacts:"
ls -lah "${BUILDBUDDY_ARTIFACTS_DIRECTORY}"
echo "=================================================="
