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

c_yellow="\x1b[33m"
c_reset="\x1b[0m"

# buildifier format all BUILD files
echo "Formatting WORKSPACE/BUILD files..."
buildifier -r .

echo "Formatting .go files..."
# go fmt all .go files
gofmt -w .

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
if which node prettier &>/dev/null; then
  BUILD_WORKSPACE_DIRECTORY="$(pwd)" ./tools/prettier/prettier.sh "$(which node)" "$(which prettier)" --write
else
  bazel run //tools/prettier:fix
fi

if ((GO_DEPS)); then
  echo "Fixing go.mod, go.sum, and deps.bzl..."
  GAZELLE_PATH=$(which gazelle || true) ./tools/fix_go_deps.sh
fi

if ((GAZELLE)); then
  echo "Fixing BUILD deps with gazelle..."
  if which gazelle &>/dev/null; then
    gazelle
  else
    bazel run //:gazelle
  fi
fi

echo 'All done!'
