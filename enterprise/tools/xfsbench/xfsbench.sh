#!/usr/bin/env bash

exec sudo --preserve-env ./enterprise/tools/xfsbench/tempxfs.sh \
  /mnt/tempxfs \
  100 \
  bash -c "
sudo mkdir -p /mnt/tempxfs/cache
sudo chown $USER:$USER /mnt/tempxfs/cache
sudo -u $USER -- bazel run //enterprise/tools/xfsbench:xfsbench -- \$@
" -- "$@"
