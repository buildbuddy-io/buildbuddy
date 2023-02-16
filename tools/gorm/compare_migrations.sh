#!/usr/bin/env bash

# This script can be used to determine whether there will be any gorm db schema changes from a git branch
#
# It assumes that the schema of the data source is already up-to-date with what is defined on the current branch,
# so the current branch can serve as a benchmark for the current schema of the db
#
# The script generates the gorm auto-migration SQL statements for the current branch, then compares these statements to the
# auto-migration SQL statements for the input branch and prints any diff
# It does not execute any migrations on the db

git_branch=$1
data_source=$2

tmpfile="$(mktemp /tmp/auto_migration.XXXXXXXXX)"
bazel run print_auto_migration -- --data_source "$data_source" --output_path "$tmpfile"

git checkout "$git_branch"
tmpfile2="$(mktemp /tmp/auto_migration.XXXXXXXXX)"
bazel run print_auto_migration -- --data_source "$data_source" --output_path "$tmpfile2"

DIFF=$(diff "$tmpfile" "$tmpfile2")
echo "$DIFF"
export DIFF

git checkout -
rm "$tmpfile" "$tmpfile2"