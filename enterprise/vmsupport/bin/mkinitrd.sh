#!/bin/bash
set -e

RANDOM_STR=$(
  tr -dc A-Za-z0-9 </dev/urandom | head -c 13
  echo ''
)-init
ROOT_DIR="${RANDOM_STR}"
mkdir -p "${ROOT_DIR}"
cp "${GOINIT}" "${ROOT_DIR}/init"
cp "${VMVFS}" "${ROOT_DIR}/vmvfs"

abspath() {
  if ! [[ "$1" =~ ^/ ]]; then
    echo "$PWD/$1"
  else
    echo "$1"
  fi
}

FSPATH="$(abspath "$1")"
CPIO="$(abspath "$CPIO")"
(
  cd "$ROOT_DIR"
  "$CPIO" -out "$FSPATH" -- init vmvfs
)
