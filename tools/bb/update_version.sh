#!/usr/bin/env bash
#
# Updates the prebuilt bb CLI version used by //tools/lint.
#
# Usage:
#   tools/bb/update_version.sh           # uses latest release
#   tools/bb/update_version.sh 5.0.333   # uses specific version
#
# This fetches the .sha256 files from the GitHub release and updates
# BB_CLI_VERSION + integrity hashes in deps.bzl.

set -euo pipefail

if [[ $# -ge 1 ]]; then
  version="$1"
else
  version=$(curl -fsSL https://api.github.com/repos/buildbuddy-io/bazel/releases/latest | perl -nle 'print $1 if /"tag_name":\s*"([^"]+)"/')
  if [[ -z "$version" ]]; then
    echo >&2 "Failed to determine latest release"
    exit 1
  fi
  echo "Latest release: ${version}"
fi
WORKSPACE_DIR="${BUILD_WORKSPACE_DIRECTORY:-$(cd "$(dirname "$0")/../.." && pwd)}"
DEPS_BZL="${WORKSPACE_DIR}/deps.bzl"

platforms=(darwin-arm64 darwin-x86_64 linux-arm64 linux-x86_64)

declare -A sums
for platform in "${platforms[@]}"; do
  url="https://github.com/buildbuddy-io/bazel/releases/download/${version}/bazel-${version}-${platform}.sha256"
  hex=$(curl -fsSL "$url" | awk '{print $1}')
  if [[ -z "$hex" ]]; then
    echo >&2 "Failed to fetch sha256 for ${platform}"
    exit 1
  fi
  b64=$(echo -n "$hex" | xxd -r -p | base64 -w0 2>/dev/null || echo -n "$hex" | xxd -r -p | base64)
  sums[$platform]="sha256-${b64}"
  echo "${platform}: ${sums[$platform]}"
done

export SUM_darwin_arm64="${sums[darwin-arm64]}"
export SUM_darwin_x86_64="${sums[darwin-x86_64]}"
export SUM_linux_arm64="${sums[linux-arm64]}"
export SUM_linux_x86_64="${sums[linux-x86_64]}"

python3 - "$DEPS_BZL" "$version" <<'PY'
import os
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
version = sys.argv[2]
content = path.read_text()
content, n = re.subn(
    r'^BB_CLI_VERSION = ".*"$',
    f'BB_CLI_VERSION = "{version}"',
    content,
    count=1,
    flags=re.MULTILINE,
)
if n != 1:
    raise SystemExit("failed to update BB_CLI_VERSION")

for platform, integrity in {
    "darwin-arm64": os.environ["SUM_darwin_arm64"],
    "darwin-x86_64": os.environ["SUM_darwin_x86_64"],
    "linux-arm64": os.environ["SUM_linux_arm64"],
    "linux-x86_64": os.environ["SUM_linux_x86_64"],
}.items():
    repo_name = f'io_buildbuddy_bb_cli-{platform}'
    pattern = rf'(name = "{re.escape(repo_name)}",.*?\n\s*executable = True,\n\s*integrity = ")sha256-[^"]*(")'
    content, n = re.subn(pattern, rf'\1{integrity}\2', content, count=1, flags=re.DOTALL)
    if n != 1:
        raise SystemExit(f"failed to update integrity for {repo_name}")

path.write_text(content)
PY

echo
echo "Updated deps.bzl to bb CLI version ${version}"
