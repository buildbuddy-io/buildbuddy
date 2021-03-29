#!/bin/bash
set -ex

# Usage ./commands/cycle.sh --all

# Execute from the /terraform/eks-cluster directory.
WORKING_DIRECTORY="$(cd $(dirname "$0");pwd)/../"
cd $WORKING_DIRECTORY

# Spin up app/cluster and run a build
$WORKING_DIRECTORY/commands/create.sh "$@"

# Destroy the app/cluster
$WORKING_DIRECTORY/commands/destroy.sh "$@"
