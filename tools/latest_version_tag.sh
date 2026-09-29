#!/usr/bin/env bash
set -e

# Prints the highest BuildBuddy version tag in the repo, like "v2.12.8".
# Sort by version, not creation date: a recent patch may be for an older minor.
# See branch_latest_version_tag.sh for the version of the current checkout.
git tag -l 'v*' --sort=version:refname |
    perl -nle 'if (/^v\d+\.\d+\.\d+$/) { print $_ }' |
    tail -n1
