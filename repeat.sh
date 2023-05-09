#!/bin/bash
for i in {1..50}
do
   echo "$i"
   /Users/maggielou/bb/buildbuddy/bazel-bin/cli/cmd/bb/bb_/bb remote test //... --config=workflows --test_tag_filters=-performance,-webdriver,-docker --remote_header=x-buildbuddy-api-key=7l2wZJyTUyhXkj381AIx
done
