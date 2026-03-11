#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." && pwd)"

TARGET="//cli/cmd/bb_analyze_profile:bb-analyze-profile"
MCP_FILE="${REPO_ROOT}/cli/mcp/mcptools/mcptools.go"
GCS_PREFIX="gs://buildbuddy-tools/binaries/bb-analyze-profile"

cd "${REPO_ROOT}"

bb build --config=static "${TARGET}"

BIN_PATH="$(bb cquery --config=static --output=files "${TARGET}" | head -n1)"
if [[ -z "${BIN_PATH}" ]]; then
  echo "could not resolve output path for ${TARGET}" >&2
  exit 1
fi
if [[ "${BIN_PATH}" != /* ]]; then
  BIN_PATH="${REPO_ROOT}/${BIN_PATH}"
fi
if [[ ! -f "${BIN_PATH}" ]]; then
  echo "built binary not found at ${BIN_PATH}" >&2
  exit 1
fi

SHA256="$(sha256sum "${BIN_PATH}" | awk '{print $1}')"
OS_NAME="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH_NAME="$(uname -m)"
PLATFORM="${OS_NAME}-${ARCH_NAME}_musl"
DEST="${GCS_PREFIX}/bb-analyze-profile_${PLATFORM}_${SHA256}"

gsutil cp "${BIN_PATH}" "${DEST}"

sed -i -E "s#(defaultAnalyzeProfileBinarySHA256 = \")[0-9a-f]{64}(\")#\\1${SHA256}\\2#" "${MCP_FILE}"

echo "binary: ${BIN_PATH}"
echo "sha256: ${SHA256}"
echo "uploaded: ${DEST}"
echo "updated: ${MCP_FILE}"
