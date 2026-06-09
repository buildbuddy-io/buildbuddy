#!/usr/bin/env bash
set -euo pipefail

for spec in "$@"; do
  version="${spec%%=*}"
  bazel="${spec#*=}"
  output_user_root="$(mktemp -d)"
  trap 'rm -rf "$output_user_root"' EXIT

  output="$("$bazel" --output_user_root="$output_user_root" version)"
  if ! grep -Fqx "Build label: ${version}" <<<"$output"; then
    echo "Expected ${bazel} to report Build label: ${version}" >&2
    echo "$output" >&2
    exit 1
  fi

  rm -rf "$output_user_root"
  trap - EXIT
done
