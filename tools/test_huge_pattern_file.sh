#!/usr/bin/env bash
set -euo pipefail

cd $(mktemp -d)
WORKDIR="$PWD"
trap 'rm -rf "$WORKDIR"' EXIT

touch MODULE.bazel
touch BUILD
echo >test.sh "sleep 1"
chmod +x test.sh

echo >&2 "Generating targets..."
rm -rf /tmp/target_patterns.txt
python3 -c '
import random
import string

characters = string.ascii_letters + string.digits

with open("/tmp/target_patterns.txt", "w") as target_patterns:
  with open("BUILD", "w") as build:
    for i in range(30000):
      name = "".join(random.choices(characters, k=1000))
      build.write("sh_test(name = \"%s\", srcs = [\"test.sh\"], env = {\"SALT\": \"%d\"})\n" % (name, random.randint(0, 1000000)))
      target_patterns.write("//:%s\n" % name)
'
du -h /tmp/target_patterns.txt
bazel test \
  --target_pattern_file=/tmp/target_patterns.txt \
  --bes_backend=grpc://localhost:1985 \
  --bes_results_url=http://localhost:8080 \
  --test_output=errors
