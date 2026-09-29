#!/usr/bin/env bash
set -e

# Prints the highest BuildBuddy version tag in HEAD's history, like "v2.12.8".
# On a release branch, this is the branch's own version even if newer versions
# have been released from later branches.
git tag -l 'v*' --sort=version:refname --merged HEAD |
    perl -nle 'if (/^v\d+\.\d+\.\d+$/) { print $_ }' |
    tail -n1
