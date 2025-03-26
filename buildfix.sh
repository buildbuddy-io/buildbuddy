#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

# If ~/go/bin exists, make sure we respect it
export PATH="$PATH:$HOME/go/bin"

GAZELLE=0
GO_DEPS=0
while [[ $# -gt 0 ]]; do
  case $1 in
  -a | --all)
    GAZELLE=1
    GO_DEPS=1
    ;;
  -g | --gazelle)
    GAZELLE=1
    ;;
  -d | --go_deps)
    GO_DEPS=1
    ;;
  esac
  shift
done

BAZEL_QUIET_FLAGS=(
  "--ui_event_filters=-info,-stdout,-stderr"
  "--noshow_progress"
)

# buildifier format all BUILD files
echo "Formatting WORKSPACE/BUILD files..."
bazel run "${BAZEL_QUIET_FLAGS[@]}" :buildifier

echo "Building and running goimports..."
GO_SRCS=()
while IFS= read -r line; do
  GO_SRCS+=("$line")
done < <(git ls-files '*.go')
bazel run "${BAZEL_QUIET_FLAGS[@]}" //tools/goimports -- -w "${GO_SRCS[@]}"

echo "Formatting .proto files..."
protos=()
while read -r proto; do
  protos+=("$PWD/$proto")
done < <(git ls-files --exclude-standard | grep '\.proto$')
if [ ${#protos[@]} -gt 0 ]; then
  bazel run "${BAZEL_QUIET_FLAGS[@]}" //tools/clang-format -- -i --style=Google "${protos[@]}"
fi

echo "Formatting frontend and markup files with prettier..."
bazel run "${BAZEL_QUIET_FLAGS[@]}" //tools/prettier -- --write

if ((GO_DEPS)); then
  echo "Fixing go.mod, go.sum, deps.bzl, and MODULE.bazel..."
  ./tools/fix_go_deps.sh
fi

if ((GAZELLE)); then
  echo "Fixing BUILD deps with gazelle..."
  CLI_VERSION="5.0.179" # Update this to latest version from `git tag -l 'cli-*' --sort=creatordate | tail -n1`
  USE_BAZEL_VERSION="buildbuddy-io/$CLI_VERSION" bazel fix
fi

echo 'All done!'
