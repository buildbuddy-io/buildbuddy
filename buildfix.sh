#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

# If ~/go/bin exists, make sure we respect it
export PATH="$PATH:$HOME/go/bin"

gazelle=$([[ "${1:-}" == '-g' ]] || [[ "${1:-}" == '--gazelle' ]] && echo 1 || echo 0)

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

if ((gazelle)); then
  echo "Fixing BUILD deps with gazelle..."
  if which gazelle &>/dev/null; then
    gazelle
  else
    bazel run //:gazelle
  fi
fi

echo 'All done!'
