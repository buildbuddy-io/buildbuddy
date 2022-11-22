#!/usr/bin/env bash
set -e

VERSION="${1?missing version argument}"

# Set UPDATE_GIT=1 env var to automatically commit, push, and create a PR.
: "${UPDATE_GIT:=0}"

if [[ ! -e .git ]]; then
  echo >&2 "$0: must be run from workspace root"
  exit 1
fi

DOCS_FILES=(
  website/src/pages/cli.tsx
  docs/cli.md
)

if [[ "$(git status --short 2>&1)" ]]; then
  echo >&2 "Working tree is dirty; refusing to update docs."
  exit 1
fi

perl -p -i -e 's#buildbuddy-io/\d+\.\d+\.\d+#buildbuddy-io/'"$VERSION"'#g' "${DOCS_FILES[@]}"
perl -p -i -e 's#buildbuddy-io/plugins@v\d+\.\d+\.\d+#buildbuddy-io/plugins@'"$VERSION"'#g' "${DOCS_FILES[@]}"

if ! ((UPDATE_GIT)); then
  exit
fi

git add "${DOCS_FILES[@]}"
git commit -m "CLI: bump version in docs to $VERSION"
if which gh &>/dev/null; then
  gh pr create --fill --web
fi
