#!/bin/bash
set -euo pipefail

# Mounts an empty, temporary XFS filesystem to the given path.
# The script stays running until you press Ctrl+C, and then it
# unmounts and deletes the temp FS.

: "${XFS_IMAGE_SIZE:=10G}"
XFS_IMAGE_PATH="/tmp/xfs-$(uuidgen).img"

MOUNT_POINT="${1:-}"
if [[ -z "$MOUNT_POINT" ]]; then
  echo >&2 "usage: $0 <mount_point>"
  exit 1
fi
if [[ $(id -u) != 0 ]]; then
  echo >&2 "$0: must be run as root"
  exit 1
fi

mkdir -p "$MOUNT_POINT"

truncate --size="${XFS_IMAGE_SIZE}" "$XFS_IMAGE_PATH"
MOUNTED=0
trap '
  ((MOUNTED)) && umount -f $MOUNT_POINT
  rm "$XFS_IMAGE_PATH"
' EXIT

OUTPUT=$(mkfs.xfs "$XFS_IMAGE_PATH" 2>&1) || {
  echo >&2 "$OUTPUT"
  exit 1
}

mount -o loop "$XFS_IMAGE_PATH" "$MOUNT_POINT"
MOUNTED=1

mount | grep "$XFS_IMAGE_PATH" >&2
echo >&2 "Press Ctrl+C to unmount..."
sleep infinity || true
