#!/usr/bin/env bash
set -euo pipefail

bazel_tar="$1"
files_tar="$2"

require_tar_basename() {
  local tar_file="$1"
  local name="$2"
  if ! tar -tf "$tar_file" | sed 's#.*/##' | grep -Fxq "$name"; then
    echo "Missing tar entry named ${name} in ${tar_file}" >&2
    echo "Tar contents:" >&2
    tar -tf "$tar_file" >&2
    exit 1
  fi
}

reject_tar_basename() {
  local tar_file="$1"
  local name="$2"
  if tar -tf "$tar_file" | sed 's#.*/##' | grep -Fxq "$name"; then
    echo "Unexpected tar entry named ${name} in ${tar_file}" >&2
    echo "Tar contents:" >&2
    tar -tf "$tar_file" >&2
    exit 1
  fi
}

for version in 8.3.1 8.4.2 8.5.1 8.6.0; do
  require_tar_basename "$bazel_tar" "bazel-${version}"
done

for version in 6.6.0 7.7.1 8.7.0 9.0.2 9.1.1; do
  reject_tar_basename "$bazel_tar" "bazel-${version}"
done

for binary in bazelcache bazelrbe remote_runner cloudprober; do
  require_tar_basename "$files_tar" "$binary"
done
