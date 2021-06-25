#!/bin/bash
set -e

gazelle=0
push=0
while [[ "$#" -gt 0 ]]; do
  case "$1" in
  -g | --gazelle)
    gazelle=1
    ;;
  -p | --push)
    push=1
    ;;
  esac
  shift
done

c_yellow="\x1b[33m"
c_green="\x1b[32m"
c_reset="\x1b[0m"

modified_before=$(mktemp)
modified_after=$(mktemp)
cleanup() {
  rm "$modified_before" "$modified_after"
  trap cleanup EXIT
}

git diff --name-only | sort >"$modified_before"

# buildifier format all BUILD files
echo "Formatting WORKSPACE/BUILD files..."
buildifier -r .

echo "Formatting .go files..."
# go fmt all .go files
gofmt -w .

if which clang-format &>/dev/null; then
  echo "Formatting .proto files..."
  git ls-files --exclude-standard | grep '\.proto$' | xargs -d '\n' --no-run-if-empty clang-format -i --style=Google
else
  echo -e "${c_yellow}WARNING: Missing clang-format; will not format proto files.${c_reset}"
fi

if ((gazelle)); then
  echo "Running gazelle..."
  bazel run //:gazelle
fi

git diff --name-only | sort >"$modified_after"

echo 'All done!'

# If no stdin is attached, skip interactive prompts.
[ -t 0 ] || exit 0

export PATH="$PATH:$HOME/go/bin"
if ! which fzf path-extractor &>/dev/null; then
  echo -e "${c_green}TIP${c_reset}: Install fzf and path-extractor to interactively stage formatted files:"
  echo '  go get -u github.com/edi9999/path-extractor/path-extractor'
  echo '  go get -u github.com/junegunn/fzf'
  exit
fi

# Auto-commit only if there are no files staged before
# selecting files to add.
autocommit=$(git diff --staged --quiet && echo '1' || echo '0')

diff -Pdpru "$modified_before" "$modified_after" | perl -n -e '/^\+([^+].*)/ && print "./$1\n"' |
  path-extractor |
  fzf --exit-0 --prompt='Stage modified files? Select with Arrows, Tab, Shift+Tab, Enter; quit with Ctrl+C > ' --multi |
  xargs -d '\n' --no-run-if-empty git add

if ((autocommit)) && ! git diff --staged --quiet; then
  git commit -m "Fix formatting / build issues"
  if ((push)); then
    git push
  fi
fi
