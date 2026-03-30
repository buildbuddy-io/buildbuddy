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
DEPS_BZL="$(cd "$(dirname "$0")/../.." && pwd)/deps.bzl"

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

# Update version
sed -i "s/^BB_CLI_VERSION = \".*\"/BB_CLI_VERSION = \"${version}\"/" "$DEPS_BZL"

# Update integrity hashes
for platform in "${platforms[@]}"; do
  repo_name="io_buildbuddy_bb_cli-${platform}"
  # Match the integrity line inside the http_file block for this repo
  sed -i "/name = \"${repo_name}\"/,/^    )/{s|integrity = \"sha256-[^\"]*\"|integrity = \"${sums[$platform]}\"|;}" "$DEPS_BZL"
done

echo
echo "Updated deps.bzl to bb CLI version ${version}"
