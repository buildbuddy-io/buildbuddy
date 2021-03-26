#!/bin/bash
set -e

# Usage ./commands/loop.sh --all

COUNT=0
while ./commands/cycle.sh $@; do
  let COUNT=COUNT+1
  echo "===================================="
  echo "======= Looped $COUNT times! ======="
  echo "===================================="
done
