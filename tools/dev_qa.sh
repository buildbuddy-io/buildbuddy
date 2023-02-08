#!/usr/bin/env bash

# Runs a few builds against dev for QA testing.
# Clones test repos to ~/buildbuddy-qa by default.
# Set QA_ROOT to override this directory.
# Pass the -l (loadtest) flag to build tensorflow

set -e

usage() { echo "Usage: $0 [-l <loadtest>] [-c continue if build fails]" 1>&2; exit 1; }

loadtest=0
continue_with_failures=0
while getopts "lc" opt; do
    case "${opt}" in
	l)
	    loadtest=1
	    ;;
	c)
	    continue_with_failures=1
	    ;;
	*)
	    usage
	    ;;
    esac
done

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

    # Add buildbuddy rbe toolchain to WORKSPACE if it's not already in there
    if ! grep -q "io_buildbuddy_buildbuddy_toolchain" "WORKSPACE"; then
      echo \
'http_archive (
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "e899f235b36cb901b678bd6f55c1229df23fcbc7921ac7a3585d29bff2bf9cfd",
    strip_prefix = "buildbuddy-toolchain-fd351ca8f152d66fc97f9d98009e0ae000854e8f",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/fd351ca8f152d66fc97f9d98009e0ae000854e8f.tar.gz"],
)
load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")
buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy", "UBUNTU20_04_IMAGE")
buildbuddy(name = "buildbuddy_toolchain", container_image = UBUNTU20_04_IMAGE)' \
    >> WORKSPACE
    fi

    bazel clean
    "$@" || (( continue_with_failures )) # if bazel command fails, continue.
  )
}

buildbuddy_iid=$(invocation_id)
gazelle_iid=$(invocation_id)
abseil_iid=$(invocation_id)
bazel_iid=$(invocation_id)
python_iid=$(invocation_id)
tensorflow_iid=$(invocation_id)

# Go Builds
# Has --experimental_remote_cache_compression and --remote_download_minimal set
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

# C++ Build
run_test \
    https://github.com/abseil/abseil-cpp \
    abseil-cpp \
    bazel build //... \
        --cxxopt="-std=c++14" \
        --remote_executor=remote.buildbuddy.dev \
        --remote_cache=remote.buildbuddy.dev \
        --bes_backend=remote.buildbuddy.dev \
        --bes_results_url=https://app.buildbuddy.dev/invocation/ \
        --remote_timeout=10m \
        --extra_execution_platforms=@buildbuddy_toolchain//:platform \
        --host_platform=@buildbuddy_toolchain//:platform \
        --platforms=@buildbuddy_toolchain//:platform \
        --crosstool_top=@buildbuddy_toolchain//:toolchain \
        --invocation_id="$abseil_iid"

# Java Build
run_test \
    https://github.com/bazelbuild/bazel \
    bazel \
    bazel build //src/main/java/com/google/devtools/build/lib/... \
        --remote_executor=remote.buildbuddy.dev \
        --remote_cache=remote.buildbuddy.dev \
        --bes_backend=remote.buildbuddy.dev \
        --bes_results_url=https://app.buildbuddy.dev/invocation/ \
        --remote_timeout=10m \
        --extra_execution_platforms=@buildbuddy_toolchain//:platform \
        --host_platform=@buildbuddy_toolchain//:platform \
        --platforms=@buildbuddy_toolchain//:platform \
        --crosstool_top=@buildbuddy_toolchain//:toolchain \
        --noincompatible_disallow_empty_glob \
        --java_runtime_version=remotejdk_17 \
        --jobs=100 \
       --invocation_id="$bazel_iid"

# Python Builds
run_test \
    https://github.com/bazelbuild/rules_python \
    rules_python \
    bazel build //... \
        --remote_executor=remote.buildbuddy.dev \
        --remote_cache=remote.buildbuddy.dev \
        --bes_backend=remote.buildbuddy.dev \
        --bes_results_url=https://app.buildbuddy.dev/invocation/ \
        --remote_timeout=10m \
        --jobs=100 \
        --invocation_id="$python_iid"

(( loadtest )) && run_test \
    https://github.com/tensorflow/tensorflow \
    tensorflow \
    bazel build tensorflow \
        --config=rbe_cpu_linux \
        --config=monolithic \
        --remote_executor=remote.buildbuddy.dev \
        --remote_cache=remote.buildbuddy.dev \
        --bes_backend=remote.buildbuddy.dev \
        --bes_results_url=https://app.buildbuddy.dev/invocation/ \
        --invocation_id="$tensorflow_iid" \
        --nogoogle_default_credentials

echo "---"
echo "QA results:"
echo "- BuildBuddy test:   https://app.buildbuddy.dev/invocation/$buildbuddy_iid"
echo "- Gazelle (Go) build:     https://app.buildbuddy.dev/invocation/$gazelle_iid"
echo "- Abseil (C++) build:     https://app.buildbuddy.dev/invocation/$abseil_iid"
echo "- Bazel (Java) build:     https://app.buildbuddy.dev/invocation/$bazel_iid"
echo "- Python build:     https://app.buildbuddy.dev/invocation/$python_iid"
(( loadtest )) && echo "- Tensorflow build:  https://app.buildbuddy.dev/invocation/$tensorflow_iid"
