#!/bin/bash
set -e

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
  clang-format -i --style=Google $(git ls-files --exclude-standard | grep '\.proto$')
else
  echo -e "${c_yellow}WARNING: Missing clang-format; will not format proto files.${c_reset}"
fi

echo "All Done!"
