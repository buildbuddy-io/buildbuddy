#!/usr/bin/env bash

# Test runner for dev QA tests against external repositories.
# This script clones external repos, checks out specific commits, and runs
# Bazel commands against them using BuildBuddy remote execution.

set -euo pipefail

# MARK - Parse Arguments

# Accept configuration from environment variables or command-line arguments
# Environment variables are used as defaults; command-line args override them
REPO_NAME="${DEV_QA_REPO_NAME:-}"
REPO_URL="${DEV_QA_REPO_URL:-}"
COMMIT_SHA="${DEV_QA_COMMIT_SHA:-}"
BAZEL_CMD="${DEV_QA_BAZEL_CMD:-}"

while [[ $# -gt 0 ]]; do
  case $1 in
    --repo-name)
      REPO_NAME="$2"
      shift 2
      ;;
    --repo-url)
      REPO_URL="$2"
      shift 2
      ;;
    --commit-sha)
      COMMIT_SHA="$2"
      shift 2
      ;;
    --bazel-cmd)
      BAZEL_CMD="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

# Validate required arguments
if [[ -z "${REPO_NAME}" ]]; then
  echo "Error: REPO_NAME not set (use --repo-name or DEV_QA_REPO_NAME env var)" >&2
  exit 1
fi
if [[ -z "${REPO_URL}" ]]; then
  echo "Error: REPO_URL not set (use --repo-url or DEV_QA_REPO_URL env var)" >&2
  exit 1
fi
if [[ -z "${COMMIT_SHA}" ]]; then
  echo "Error: COMMIT_SHA not set (use --commit-sha or DEV_QA_COMMIT_SHA env var)" >&2
  exit 1
fi
if [[ -z "${BAZEL_CMD}" ]]; then
  echo "Error: BAZEL_CMD not set (use --bazel-cmd or DEV_QA_BAZEL_CMD env var)" >&2
  exit 1
fi

# MARK - Environment Setup

# QA_ROOT is where we clone and test external repositories
# When running as a Bazel test, use TEST_TMPDIR (sandbox-safe location)
# Otherwise default to ${HOME}/buildbuddy-qa for local development
if [[ -n "${TEST_TMPDIR:-}" ]]; then
  QA_ROOT="${QA_ROOT:-${TEST_TMPDIR}/buildbuddy-qa}"
else
  QA_ROOT="${QA_ROOT:-${HOME}/buildbuddy-qa}"
fi

# API key must be set in environment
if [[ -z "${BB_API_KEY:-}" ]]; then
  echo "Error: BB_API_KEY environment variable must be set" >&2
  exit 1
fi
API_KEY="${BB_API_KEY}"

# BIT_BAZEL_BINARY is set by rules_bazel_integration_test
if [[ -z "${BIT_BAZEL_BINARY:-}" ]]; then
  echo "Error: BIT_BAZEL_BINARY not set. This script should be run via script_test." >&2
  exit 1
fi
BAZEL="${BIT_BAZEL_BINARY}"

# Generate a unique invocation ID for this test run
INVOCATION_ID="$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "test-$(date +%s)")"

# MARK - Repository Setup

echo "=========================================="
echo "Dev QA Test: ${REPO_NAME}"
echo "Repository: ${REPO_URL}"
echo "Commit: ${COMMIT_SHA}"
echo "Invocation ID: ${INVOCATION_ID}"
echo "=========================================="

# Create QA root directory if it doesn't exist
mkdir -p "${QA_ROOT}"
cd "${QA_ROOT}"

# Clone repository if it doesn't exist, otherwise update it
if [[ ! -d "${REPO_NAME}" ]]; then
  echo "Cloning ${REPO_NAME}..."
  git clone "${REPO_URL}" "${REPO_NAME}"
else
  echo "Repository ${REPO_NAME} already exists, fetching updates..."
  cd "${REPO_NAME}"
  git fetch
  cd "${QA_ROOT}"
fi

cd "${REPO_NAME}"

# Checkout the specific commit
echo "Checking out commit ${COMMIT_SHA}..."
git checkout "${COMMIT_SHA}"

# MARK - BuildBuddy Toolchain Injection

# Define the BuildBuddy toolchain snippet to inject
BUILDBUDDY_TOOLCHAIN_SNIPPET='
http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    integrity = "sha256-VtJjefgP2Vq5S6DiGYczsupNkosybmSBGWwcLUAYz8c=",
    strip_prefix = "buildbuddy-toolchain-66146a3015faa348391fcceea2120caa390abe03",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/66146a3015faa348391fcceea2120caa390abe03.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "UBUNTU20_04_IMAGE", "buildbuddy")

buildbuddy(
    name = "buildbuddy_toolchain",
    container_image = UBUNTU20_04_IMAGE,
    # This is the MSVC available on Github Action win22 image
    msvc_edition = "Enterprise",
    msvc_release = "2022",
    msvc_version = "14.44.35207",
)

register_toolchains(
    "@buildbuddy_toolchain//:all",
)
'

# Add BuildBuddy RBE toolchain to WORKSPACE if it's not already there
if [[ -f WORKSPACE ]] && ! grep -q "io_buildbuddy_buildbuddy_toolchain" WORKSPACE; then
  echo "Injecting BuildBuddy toolchain into WORKSPACE..."
  echo "${BUILDBUDDY_TOOLCHAIN_SNIPPET}" >> WORKSPACE
fi

# Pin to a specific Bazel version for third-party repos
if [[ "${REPO_NAME}" != "buildbuddy" ]]; then
  echo "Pinning Bazel version to 7.4.0 for third-party repo..."
  echo '7.4.0' > .bazelversion
fi

# MARK - Run Bazel Command

echo "Running Bazel command..."
echo "Command: ${BAZEL_CMD}"

# Clean before running the test
"${BAZEL}" clean

# Parse the command string into an array
# Note: This is a simple split on spaces - if you have quoted args with spaces,
# you'd need more sophisticated parsing
IFS=' ' read -ra CMD_PARTS <<< "${BAZEL_CMD}"

# Run the Bazel command with the invocation ID
set -x
"${BAZEL}" "${CMD_PARTS[@]}" \
  --remote_header=x-buildbuddy-api-key="${API_KEY}" \
  --invocation_id="${INVOCATION_ID}"
EXIT_CODE=$?
set +x

# MARK - Report Results

INVOCATION_URL="https://app.buildbuddy.dev/invocation/${INVOCATION_ID}"

echo ""
echo "=========================================="
echo "Test completed for ${REPO_NAME}"
echo "Exit code: ${EXIT_CODE}"
echo "Results: ${INVOCATION_URL}"
echo "=========================================="

exit ${EXIT_CODE}
