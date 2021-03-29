#!/bin/bash
set -e

# Usage ./commands/loop.sh --all

# Execute from the /terraform/eks-cluster directory.
cd "$(cd $(dirname "$0");pwd)/../../"

COUNT=0
while ./commands/cycle.sh "$@"; do
  let COUNT=COUNT+1
  echo "===================================="
  echo "========== Looped $COUNT times! ========="
  echo "===================================="
done
