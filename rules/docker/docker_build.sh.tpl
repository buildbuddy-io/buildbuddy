#!/bin/bash 

set -euox pipefail

docker buildx build --no-cache -t {image_name} -f {dockerfile} .

docker save {image_name} | gzip > {archive}
