#!/usr/bin/env bash

# This script generates a Bazel-compatible repository for Tensorflow. Currently
# the repository contents are uploaded to a blank-slate git repo to avoid
# pulling in the entire history and unnecessary sources from tensorflow proper.
# The repo is hosted at github.com/bduffany/go-tensorflow currently, but this
# can be forked as needed.
#
# Usage:
#
# enterprise/tools/tensorflow/create_repository.sh ~/local/clone/of/github.com/bduffany/go-tensorflow

set -e

: "${TENSORFLOW_VERSION:=2.9.0}"
REPO_DIR="${1:?}"

if ! (cd "$REPO_DIR" && git status &>/dev/null); then
  echo "$REPO_DIR is not a git repo"
fi

docker build \
  --tag tensorflow/generate \
  --build-arg="TENSORFLOW_VERSION=$TENSORFLOW_VERSION" \
  enterprise/tools/tensorflow

echo "Will generate new, bazel-compatible repo contents under $REPO_DIR based on v$TENSORFLOW_VERSION"
(
  cd "$REPO_DIR"
  if ! git diff --quiet; then
    echo >&2 "Working tree is dirty; exiting."
    exit 1
  fi
  # TODO: Allow basing on a previous version, so repo doesn't get bloated
  # from having independent branches for each version.
  git checkout main
  git checkout -b "v$TENSORFLOW_VERSION" || git checkout "v$TENSORFLOW_VERSION"
  # Clear the current branch contents; we'll re-populate it from the Docker build.
  find . -not -path "./.git/*" -not -name ".git" -delete
)

docker run \
  --rm \
  --volume="$REPO_DIR:/out" \
  --workdir=/tensorflow \
  --user="$(id -u):$(id -g)" \
  tensorflow/generate \
  sh -c 'cp -r ./* /out/'

(
  cd "$REPO_DIR"
  git add .
  git commit -m "Generate v$TENSORFLOW_VERSION"
  git push --set-upstream origin --force "$(git branch --show-current)"
)
