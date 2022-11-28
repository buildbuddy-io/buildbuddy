#!/usr/bin/env bash

install_buildbuddy_cli() (
  set -eo pipefail

  arch=$(uname -m) # x86_64 | arm64
  os=$(uname -s)   # Linux | Darwin
  os=$(tr '[:upper:]' '[:lower:]' <<<"$os")
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
