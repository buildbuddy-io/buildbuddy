#!/bin/bash
set -e

GOINIT=$1  # path to goinit binary
FSPATH=$2  # path to write output filesytem to

RANDOM_STR=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13 ; echo '')-init
ROOT_DIR=${RANDOM_STR}

mkdir -p ${ROOT_DIR}/bb
cp ${GOINIT} ${ROOT_DIR}/bb/init

DIR_SIZE=$(du -sk ${ROOT_DIR} | awk '{print $1}')
IMAGE_SIZE_KBYTES=$(echo "${DIR_SIZE} * 1.4 / 1" | bc)
echo "dir size is: $DIR_SIZE image_size is: $IMAGE_SIZE_KBYTES K"
IMAGE_FILE=${RANDOM_STR}.ext2

rm -f "${IMAGE_FILE}"
/sbin/mke2fs \
  -L '' \
  -N 0 \
  -O ^64bit \
  -d "${ROOT_DIR}" \
  -m 1 \
  -r 1 \
  -t ext2 \
  "${FSPATH}" \
  ${IMAGE_SIZE_KBYTES}k \
;

echo "Created initfs: ${FSPATH}"
