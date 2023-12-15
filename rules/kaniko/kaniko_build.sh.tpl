#!/bin/bash 

set -euxo pipefail

mkdir -p /tmp/kaniko-cache

{kaniko} \
  --log-timestamp \
  --image-fs-extract-retry=3 \
  --image-download-retry=3 \
  --skip-tls-verify \
  --registry-mirror={registry_mirror} \
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
