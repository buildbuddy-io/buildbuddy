#!/usr/bin/env bash

# Runs a few builds against dev for QA testing.
# Clones test repos to ~/buildbuddy-qa by default.
# Set QA_ROOT to override this directory.

set -e

: "${QA_ROOT:=$HOME/buildbuddy-qa}"

mkdir -p "$QA_ROOT"
cd "$QA_ROOT"

invocation_id() {
  uuidgen | tr '[:upper:]' '[:lower:]'
}

run_test() {
  url="$1"
  shift
  dir="$1"
  shift
  if ! [[ -e "$dir" ]]; then
    git clone "$url" "$dir"
  fi
  (
    cd "$dir"
    "$@" || true # if bazel command fails, continue.
  )
}

buildbuddy_iid=$(invocation_id)
gazelle_iid=$(invocation_id)
tensorflow_iid=$(invocation_id)

run_test \
    https://github.com/buildbuddy-io/buildbuddy \
    buildbuddy \
    bazel test //... \
        --config=remote-dev \
        --invocation_id="$buildbuddy_iid"

run_test \
    https://github.com/bazelbuild/bazel-gazelle \
    bazel-gazelle \
    bazel build //... \
        --remote_executor=remote.buildbuddy.dev \
        --remote_cache=remote.buildbuddy.dev \
        --bes_backend=remote.buildbuddy.dev \
        --bes_results_url=https://app.buildbuddy.dev/invocation/ \
        --remote_timeout=10m \
        --invocation_id="$gazelle_iid"

run_test \
    https://github.com/tensorflow/tensorflow \
    tensorflow \
    bazel build tensorflow \
        --config=rbe_cpu_linux \
        --config=monolithic \
        --remote_executor=remote.buildbuddy.dev \
        --remote_cache=remote.buildbuddy.dev \
        --bes_backend=remote.buildbuddy.dev \
        --bes_results_url=https://app.buildbuddy.dev/invocation/ \
        --invocation_id="$tensorflow_iid"

echo "---"
echo "QA results:"
echo "- BuildBuddy test:   https://app.buildbuddy.dev/invocation/$buildbuddy_iid"
echo "- Gazelle build:     https://app.buildbuddy.dev/invocation/$gazelle_iid"
echo "- Tensorflow build:  https://app.buildbuddy.dev/invocation/$tensorflow_iid"

