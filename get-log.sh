#!/bin/sh -x

(
  cd ${BUILD_WORKING_DIRECTORY}
  cp "$(bazel info output_base | sed 's/\r//')/java.log" .
)
