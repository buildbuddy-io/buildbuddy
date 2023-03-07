#!/bin/sh -x

(
  cd ${BUILD_WORKING_DIRECTORY}
  cp "$(/root/workspace/bazelisk --output_base=/root/workspace/output-base --bazelrc=/root/workspace/buildbuddy.bazelrc --noworkspace_rc --bazelrc=.bazelrc --bazelrc=custom.bazelrc info output_base | sed 's/\r//')/java.log" .
)
