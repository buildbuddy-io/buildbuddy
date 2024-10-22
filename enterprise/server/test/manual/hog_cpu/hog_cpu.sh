#!/usr/bin/env bash
set -eu

HOGS=1
RAND=$(od -An -N2 -i /dev/urandom)
if ((RAND % 100 < HOG_PERCENT)); then
  HOGS=$(nproc)
fi

echo "HOGS=$HOGS"
for _ in $(seq 1 $HOGS); do
  python3 -S -c '
for _ in range(200_000_000):
  pass
' &
done
wait
