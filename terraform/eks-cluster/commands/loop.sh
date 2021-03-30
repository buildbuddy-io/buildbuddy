#!/bin/bash
set -e

# Usage ./commands/loop.sh --all

# Execute from the /terraform/eks-cluster directory.
WORKING_DIRECTORY="$(cd $(dirname "$0");pwd)/.."
cd $WORKING_DIRECTORY

COUNT=0
while $WORKING_DIRECTORY/commands/cycle.sh "$@"; do
  let COUNT=COUNT+1
  echo "===================================="
  echo "========== Looped $COUNT times! ========="
  echo "===================================="
done
