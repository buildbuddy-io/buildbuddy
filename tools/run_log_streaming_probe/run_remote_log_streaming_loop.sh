#!/usr/bin/env bash
set -euo pipefail

for i in $(seq 1); do
	bb remote --remote_runner=app.buildbuddy.dev --os=linux --arch=amd64 run --stream_run_logs --on_stream_run_logs_failure=fail //tools/run_log_streaming_probe:log_spammer --remote_header=x-buildbuddy-api-key=ko8p4RC7aIx3wXOJ1pbO
done
