#!/usr/bin/env bash

set -e

# This script can be used to determine whether there will be any gorm db schema changes from a git branch
#
# It assumes that the input data source has the db schema from the master branch
# It generates the gorm auto-migration SQL statements on the master branch, then compares these statements to the
# SQL statements on the input branch and prints any diff
# It does not execute any migrations on the db

git_branch=$1
data_source=$2

git checkout master
tmpfile="$(mktemp /tmp/auto_migration.XXXXXXXXX)"
bazel run print_auto_migration -- --data_source "$data_source" --output_path "$tmpfile"

git checkout "$git_branch"
tmpfile2="$(mktemp /tmp/auto_migration.XXXXXXXXX)"
bazel run print_auto_migration -- --data_source "$data_source" --output_path "$tmpfile2"

#TODO: Delete tmp files

diff "$tmpfile" "$tmpfile2"

