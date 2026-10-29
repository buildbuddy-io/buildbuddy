#!/usr/bin/env bash

install_buildbuddy_cli() (
  set -eo pipefail

  # Get host CPU architecture: "x86_64" or "arm64"
  arch=$(uname -m)
  if [[ "$arch" == "aarch64" ]]; then arch="arm64"; fi

  # Get host OS name: "linux" or "darwin"
  os=$(uname -s | tr '[:upper:]' '[:lower:]')

  tempfile=$(mktemp buildbuddy.XXXXX)
  checksumfile=""
  cleanup() { rm -f "$tempfile" "$checksumfile"; }
  trap cleanup EXIT
  checksumfile=$(mktemp buildbuddy.XXXXX)

  # Look for the line matching
  #   "browser_download_url": "https://github.com/buildbuddy-io/bazel/releases/.../bazel-...-${os}-${arch}"
  # and extract the URL.
  release="${1:-latest}"
  latest_binary_url=$(
    curl --proto '=https' --tlsv1.2 -fsSL https://api.github.com/repos/buildbuddy-io/bazel/releases/"$release" |
      perl -nle 'if (/"browser_download_url":\s*"(.*?-'"${os}-${arch}"')"/) { print $1 }'
  )

  if [[ ! "$latest_binary_url" ]]; then
    echo >&2 "Could not find a CLI release for os '$os', arch '$arch'"
    exit 1
  fi

  echo >&2 "Downloading $latest_binary_url"
  curl --proto '=https' --tlsv1.2 -fSL "$latest_binary_url" -o "$tempfile"
  # A same-host .sha256 detects corruption, not a malicious host.
  curl --proto '=https' --tlsv1.2 -fSL "$latest_binary_url.sha256" -o "$checksumfile"
  if ! read -r expected_hash checksum_filename checksum_extra < "$checksumfile" ||
    [[ ! "$expected_hash" =~ ^[[:xdigit:]]{64}$ || "$checksum_filename" != "${latest_binary_url##*/}" || -n "$checksum_extra" ]]; then
    echo >&2 "Invalid SHA-256 checksum or filename for ${latest_binary_url##*/}"
    exit 1
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    actual_hash=$(sha256sum "$tempfile")
  elif command -v shasum >/dev/null 2>&1; then
    actual_hash=$(shasum -a 256 "$tempfile")
  else
    echo >&2 "SHA-256 verification requires sha256sum or shasum"
    exit 1
  fi
  expected_hash=$(printf '%s' "$expected_hash" | tr '[:upper:]' '[:lower:]')
  if [[ "${actual_hash%% *}" != "$expected_hash" ]]; then
    echo >&2 "SHA-256 checksum mismatch for ${latest_binary_url##*/}"
    exit 1
  fi
  chmod 0755 "$tempfile"
  install_path="/usr/local/bin/bb"
  install_dir=$(dirname "$install_path")
  echo >&2 "Will write the CLI to $install_dir - this may ask for your password."
  sudo mkdir -p "$install_dir"
  sudo mv "$tempfile" "$install_path"
)

install_buildbuddy_cli "$@"
