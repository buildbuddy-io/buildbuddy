#!/bin/bash
# Build BuildBuddy targets through a local actiond worker.
# Wraps `bazel --digest_function=SHA256 build --config=actiond` because the
# digest function is a startup flag (actiond requires SHA256, not BuildBuddy's
# default BLAKE3) and so cannot live in a --config.
#
#   tools/actiond/build.sh //cli/cmd/bb:bb
#   tools/actiond/build.sh test //some:target   # first arg may be a bazel verb
set -euo pipefail
cd "$(dirname "$0")/../.."
BAZEL=${BAZEL:-bazel}
verb=build
if [[ "${1:-}" =~ ^(build|test|run|query|cquery|aquery)$ ]]; then verb=$1; shift; fi
exec "$BAZEL" --digest_function=SHA256 "$verb" --config=actiond "$@"
