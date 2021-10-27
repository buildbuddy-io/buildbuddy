#!/bin/bash
set -e
cd "$(dirname "$0")"
repo="gcr.io/flame-public/rbe-webdriver"
docker build . --tag "$repo"
if [[ "$1" == "-p" ]] || [[ "$1" == "--push" ]]; then
  docker push "$repo"
fi
