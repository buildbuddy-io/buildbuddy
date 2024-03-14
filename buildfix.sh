#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

# If ~/go/bin exists, make sure we respect it
export PATH="$PATH:$HOME/go/bin"

GO_DEPS=0
while [[ $# -gt 0 ]]; do
  case $1 in
  -a | --all)
    GO_DEPS=1
    ;;
  -d | --go_deps)
    GO_DEPS=1
    ;;
  esac
  shift
done

c_yellow="\x1b[33m"
c_reset="\x1b[0m"

BAZEL_QUIET_FLAGS=(
  "--ui_event_filters=-info,-stdout,-stderr"
  "--noshow_progress"
)

echo "Building and running gofmt..."
GO_SRCS=()
while IFS= read -r line; do
  GO_SRCS+=("$line")
done < <(git ls-files '*.go')
bazel run "${BAZEL_QUIET_FLAGS[@]}" //:gofmt -- -w "${GO_SRCS[@]}"

if which clang-format &>/dev/null; then
  echo "Formatting .proto files..."
  protos=()
  while read -r proto; do
    protos+=("$proto")
  done < <(git ls-files --exclude-standard | grep '\.proto$')
  if [ ${#protos[@]} -gt 0 ]; then
    clang-format -i --style=Google "${protos[@]}"
  fi
else
  echo -e "${c_yellow}WARNING: Missing clang-format tool; will not format proto files.${c_reset}"
fi

echo "Formatting frontend and markup files with prettier..."
./tools/prettier/prettier.sh --write

if ((GO_DEPS)); then
  echo "Fixing go.mod, go.sum, and deps.bzl..."
  ./tools/fix_go_deps.sh
fi

echo "Running buildifier and gazelle with bb fix..."
CLI_VERSION="5.0.36" # Update this to latest version from `git tag -l 'cli-*' --sort=creatordate | tail -n1`
USE_BAZEL_VERSION="buildbuddy-io/$CLI_VERSION" bazel fix

echo 'All done!'
