#!/bin/bash

/root/workspace/bazelisk --output_base=/root/workspace/output-base --bazelrc=/root/workspace/buildbuddy.bazelrc --noworkspace_rc --bazelrc=.bazelrc info

echo 'Testing once'
/root/workspace/bazelisk --output_base=/root/workspace/output-base --bazelrc=/root/workspace/buildbuddy.bazelrc --noworkspace_rc --bazelrc=.bazelrc test //... --config=workflows --test_tag_filters=-performance,-webdriver,-docker

echo 'Testing again'
/root/workspace/bazelisk --output_base=/root/workspace/output-base --bazelrc=/root/workspace/buildbuddy.bazelrc --noworkspace_rc --bazelrc=.bazelrc test //... --config=workflows --test_tag_filters=-performance,-webdriver,-docker

/root/workspace/bazelisk --output_base=/root/workspace/output-base --bazelrc=/root/workspace/buildbuddy.bazelrc --noworkspace_rc --bazelrc=.bazelrc info
