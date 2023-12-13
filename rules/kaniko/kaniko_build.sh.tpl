#!/bin/bash 

set -euo pipefail

mkdir -p /tmp/kaniko-cache

{kaniko} \
  --log-timestamp \
  --dockerfile={dockerfile} \
  --context={context_directory_path} \
  --reproducible \
  --single-snapshot \
  --cache-dir=/tmp/kaniko-cache \
  --tar-path={archive} \
  --no-push \
  --no-push-cache \
  --use-new-run \
  --digest-file={digest_file}

echo "Image digest is $(cat {digest_file})"
