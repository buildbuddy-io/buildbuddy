#!/usr/bin/env bash
exec ./enterprise/server/remote_execution/containers/firecracker/test.sh \
  -- \
  -test.bench=. \
  -test.benchtime=1x \
  -test.run=^Bench \
  --app.log_level=error \
  "$@"
