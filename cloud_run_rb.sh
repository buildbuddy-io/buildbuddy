#!/usr/bin/env bash
set -e

api_key=$1
command=$2

instance_name=$(date +"%Y%m%d_%H%M%S")
bb remote --remote_runner=grpcs://remote.buildbuddy.dev --runner_exec_properties=instance_name=$instance_name --runner_exec_properties=EstimatedComputeUnits=4 --runner_exec_properties=EstimatedFreeDiskBytes=35000000000 $command --remote_header=x-buildbuddy-api-key="$api_key" --config=remote-dev
