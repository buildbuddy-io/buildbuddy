#!/bin/bash
set -ex

# Usage ./commands/cycle.sh --all

# Execute from the /terraform/eks-cluster directory.
cd "$(cd $(dirname "$0");pwd)/../../"

# Spin up app/cluster and run a build
./commands/create.sh "$@"

# Destroy the app/cluster
./commands/destroy.sh "$@"
