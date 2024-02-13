#!/usr/bin/env bash

: "${INSTALL_PATH:=/usr/local/bin}"
bazel build tools/logview
sudo bash -e -c "
  DEST=\"$INSTALL_PATH/logview\"
  [[ -e \"\$DEST\" ]] && rm -f \"\$DEST\"
  cp ./bazel-bin/tools/logview/logview_/logview \"\$DEST\"
"
