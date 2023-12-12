#!/bin/bash 

set -euo pipefail

docker buildx build --no-cache -t {image_name} -f {dockerfile} .

temp_dir=$(mktemp -d)

docker save -o ${temp_dir}/image.tar {image_name}

du -h ${temp_dir}/image.tar
df -h

cp ${temp_dir}/image.tar {archive}
