#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

# If ~/go/bin exists, make sure we respect it
export PATH="$PATH:$HOME/go/bin"

gazelle=0
stage=0
commit=0
push=0
while [[ "$#" -gt 0 ]]; do
  case "$1" in
  -g | --gazelle)
    gazelle=1
    ;;
  -s | --stage)
    stage=1
    ;;
  -c | --commit)
    stage=1
    commit=1
    ;;
  -p | --push)
    stage=1
    commit=1
    push=1
    ;;
  *)
    echo "Usage: $0 [-g] [-s | -c | -p]"
    echo ""
    echo "Fix options:"
    echo "  -g, --gazelle  Run gazelle"
    echo ""
    echo "Git options:"
    echo "  -s, --stage    Interactively stage files ('git add') after formatting."
    echo "  -c, --commit   Commit staged files. Has no effect if there are existing staged changes. Implies -s."
    echo "  -p, --push     Push any commits that are made. Implies -c."
    exit 1
    ;;
  esac

  shift
done

c_yellow="\x1b[33m"
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
  echo -e "${c_yellow}WARNING: Missing clang-format tool; will not format proto files.${c_reset}"
fi

echo "Formatting frontend and markup files with prettier..."
if which node prettier &>/dev/null; then
  BUILD_WORKSPACE_PATH="$(pwd)" ./tools/prettier/prettier.sh "$(which node)" "$(which prettier)" --write
else
  bazel run --ui_event_filters=-info,-stdout,-stderr --noshow_progress //tools/prettier:fix
fi

if ((gazelle)); then
  echo "Fixing BUILD deps with gazelle..."
  if which gazelle &>/dev/null; then
    gazelle
  else
    bazel run --ui_event_filters=-info,-stdout,-stderr --noshow_progress //:gazelle
  fi
fi

git diff --name-only | sort >"$modified_after"

echo 'All done!'

if ! ((stage)); then
  exit
fi
if ! [ -t 0 ] || ! [ -t 1 ]; then
  echo -e "${c_yellow}WARNING: not connected to a terminal; skipping interactive staging.${c_reset}"
  exit 1
fi

if ! which fzf &>/dev/null; then
  echo -e "${c_yellow}WARNING: Missing fzf tool; did not stage any changes.${c_reset}"
  exit 1
fi

# Only commit staged files if there were no files staged before running this script.
prev_stage_clean=$(git diff --staged --quiet && echo 1 || echo 0)

diff -Pdpru "$modified_before" "$modified_after" | perl -n -e '/^\+([^+].*)/ && print "./$1\n"' |
  fzf --exit-0 --prompt='Stage modified files? Select with Arrows, Tab, Shift+Tab, Enter; quit with Ctrl+C > ' --multi |
  xargs -d '\n' --no-run-if-empty git add

if ((commit)) && ! git diff --staged --quiet; then
  if ! ((prev_stage_clean)); then
    echo -e "${c_yellow}WARNING: Files will need to be manually committed since there were existing staged files.${c_reset}"
    exit 1
  fi

  git commit -m "Fix formatting / build issues (buildfix.sh)"
  if ((push)); then
    git push
  fi
fi
