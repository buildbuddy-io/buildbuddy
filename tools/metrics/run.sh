#!/bin/bash

set -e -o pipefail
__file__="$0"
__dir__=$(dirname "$__file__")

# Open grafana webpage in browser when ready
(
  open=$(which open &>/dev/null && echo "open" || echo "xdg-open")
  tries=100
  grafana_url="http://localhost:4500"
  while ! curl "$grafana_url" &>/dev/null ; do
    sleep 2
    tries=$(( tries - 1 ))
    if [[ $tries == 0 ]] ; then
      exit 1
    fi
  done
  "$open" "http://localhost:4500"
) &

# Run docker-compose
docker-compose up
