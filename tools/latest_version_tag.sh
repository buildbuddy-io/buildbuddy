#!/usr/bin/env bash
set -e

# Prints the latest BuildBuddy version tag, like "v2.12.8".
# Extra args are passed to `git tag`, e.g. `--merged HEAD`.
# Sort by version, not creation date: a recent patch may be for an older minor.
git tag -l 'v*' --sort=version:refname "$@" |
    perl -nle 'if (/^v\d+\.\d+\.\d+$/) { print $_ }' |
    tail -n1
