#!/bin/bash
set -e

GOINIT=$1  # path to goinit binary
VMVFS=$2 # path to vmvfs binary
FSPATH=$3  # path to write output filesytem to

RANDOM_STR=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13 ; echo '')-init
ROOT_DIR=${RANDOM_STR}

mkdir -p ${ROOT_DIR}
cp ${GOINIT} ${ROOT_DIR}/init
cp ${VMVFS} ${ROOT_DIR}/vmvfs
IMAGE_FILE=${RANDOM_STR}.cpio

pushd ${ROOT_DIR}
find "./" | cpio --create --format=newc -O /tmp/initrd.cpio
popd

cp /tmp/initrd.cpio "${FSPATH}"
echo "Created initrd: ${FSPATH}"
