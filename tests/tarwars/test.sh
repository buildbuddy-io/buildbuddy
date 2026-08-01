#!/usr/bin/env bash
set -o errexit -o nounset -o pipefail

mkdir -p output
tar xvzf "$1" -C output
rm -rf output
