#!/usr/bin/env bash
set -e

# Prints the latest BuildBuddy CLI version, like "5.0.328".
git tag -l 'cli-v*' --sort=creatordate |
    perl -nle 'if (/^cli-v(\d+\.\d+\.\d+)$/) { print $1 }' |
    tail -n1
