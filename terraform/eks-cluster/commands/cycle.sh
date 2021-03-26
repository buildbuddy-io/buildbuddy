#!/bin/bash
set -ex

# Usage ./commands/cycle.sh --all

# Spin up app/cluster and run a build
./commands/create.sh $@

# Destroy the app/cluster
./commands/destroy.sh $@