#!/bin/bash
set -euo pipefail

# Optional env vars
: "${AGCOUNT:=17}"

if [[ $# -lt 1 ]]; then
  echo >&2 "Usage: $0 <mount_point> [size_gb]"
  exit 1
fi

MOUNT_POINT="$1"
SIZE_GB="${2:-1}"
IMAGE_FILE=$(mktemp)

# Cleanup function to be called on exit
cleanup() {
  echo >&2
  echo >&2 "Unmounting ${MOUNT_POINT}..."
  sync
  umount "${MOUNT_POINT}" || true
  echo >&2 "Removing temporary image file ${IMAGE_FILE}..."
  rm -f "${IMAGE_FILE}"
  echo >&2 "Cleanup complete."
}

# Trap EXIT signal to ensure cleanup happens
trap cleanup EXIT

echo >&2 "Creating a ${SIZE_GB}GB image file at ${IMAGE_FILE}..."
fallocate -l "${SIZE_GB}G" "${IMAGE_FILE}"

echo >&2 "Formatting with XFS..."
mkfs.xfs -l size=2038m -d agcount="$AGCOUNT" -m finobt=0,rmapbt=0 "${IMAGE_FILE}" > /dev/null

echo >&2 "Mounting ${IMAGE_FILE} at ${MOUNT_POINT}..."
# Ensure mount point exists
mkdir -p "${MOUNT_POINT}"
sudo mount -o loop "${IMAGE_FILE}" "${MOUNT_POINT}"

echo >&2 "Successfully mounted temporary XFS filesystem at ${MOUNT_POINT}."

# Run command with the XFS mounted
shift 2
COMMAND=("$@")
# If the command is empty, default to 'sleep infinity'
if [[ "${#COMMAND[@]}" -eq 0 ]]; then
  echo >&2 "Press Ctrl+C to unmount and clean up..."
  sleep infinity
else
  echo >&2 "Running command (will unmount XFS when done): ${COMMAND[@]@Q}"
  "${COMMAND[@]}"
fi
