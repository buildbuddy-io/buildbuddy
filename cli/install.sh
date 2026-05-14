#!/usr/bin/env bash

install_buildbuddy_cli() (
  set -eo pipefail

  # Get host CPU architecture: "x86_64" or "arm64"
  arch=$(uname -m)
  if [[ "$arch" == "aarch64" ]]; then arch="arm64"; fi

  # Get host OS name: "linux" or "darwin"
  os=$(uname -s | tr '[:upper:]' '[:lower:]')

  tempfile=$(mktemp buildbuddy.XXXXX)
  cleanup() { rm -f "$tempfile"; }
  trap cleanup EXIT

  # Look for the line matching
  #   "browser_download_url": "https://github.com/buildbuddy-io/bazel/releases/.../bazel-...-${os}-${arch}"
  # and extract the URL.
  release="${1:-latest}"
  latest_binary_url=$(
    curl -fsSL https://api.github.com/repos/buildbuddy-io/bazel/releases/"$release" |
      perl -nle 'if (/"browser_download_url":\s*"(.*?-'"${os}-${arch}"')"/) { print $1 }'
  )

  if [[ ! "$latest_binary_url" ]]; then
    echo >&2 "Could not find a CLI release for os '$os', arch '$arch'"
    exit 1
  fi

  echo >&2 "Downloading $latest_binary_url"
  curl -fSL "$latest_binary_url" -o "$tempfile"
  chmod 0755 "$tempfile"
  echo >&2 "Will write the CLI to /usr/local/bin - this may ask for your password."
  sudo mv "$tempfile" /usr/local/bin/bb
)

install_buildbuddy_cli "$@"
