#!/bin/bash
set -e

cd ~/code/bbscratch/replay_logs

iid=$(uuidgen | tr '[[:upper:]]' '[[:lower:]]')

(
  sleep 2
  $(which open xdg-open) http://localhost:8080/invocation/"$iid"
) &

./replay.py --chunk_length=500 --limit=40 --delay=0.2 -s -p -- --invocation_id="$iid"
