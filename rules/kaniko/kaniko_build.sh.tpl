#!/bin/bash 

set -euo pipefail

mkdir -p /tmp/kaniko-cache

{kaniko} \
  --dockerfile={dockerfile} \
  --context={context_directory_path} \
  --reproducible \
  --single-snapshot \
  --cache-dir=/tmp/kaniko-cache \
  --tar-path={archive} \
  --no-push
