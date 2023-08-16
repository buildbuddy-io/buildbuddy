#!/usr/bin/env bash
set -e

# Installs or update firecracker to match the local version in deps.bzl.

: "${BAZEL:=bazelisk}"

if ! [[ "$SUDO_USER" ]]; then
  echo >&2 "Script must be run with sudo."
  exit 1
fi

# Build as the original user to avoid creating a bunch of files that can only be
# removed with sudo.
echo >&2 "Fetching firecracker/jailer with bazel..."
WORKSPACE_DIR=$(pwd)
su - "$SUDO_USER" -c "
  cd $WORKSPACE_DIR
  $BAZEL build //enterprise/server/cmd/executor:firecracker //enterprise/server/cmd/executor:jailer
"

cp_with_backup() {
  src="$1"
  dst="$2"
  if [[ -e "$dst" ]]; then
    if cmp --silent "$src" "$dst"; then
      return
    fi
    backup=$(mktemp --suffix "-$(basename "$dst")")
    echo >&2 "Backing up existing $dst to $backup"
    mv "$dst" "$backup"
  fi
  cp "$src" "$dst"
}

cp_with_backup bazel-bin/enterprise/server/cmd/executor/firecracker /usr/local/bin/firecracker
cp_with_backup bazel-bin/enterprise/server/cmd/executor/jailer /usr/local/bin/jailer
