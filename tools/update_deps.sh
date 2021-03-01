#!/bin/bash
set -e
cd "$(dirname "$(realpath "$0")")/.."
bazelisk run \
    //:gazelle -- update-repos -from_file=go.mod -to_macro=deps.bzl%install_buildbuddy_dependencies
