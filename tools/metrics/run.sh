#!/bin/bash

set -e -o pipefail
__file__="$0"
__dir__=$(dirname "$__file__")

cd "$__dir__"

GRAFANA_PORT=${GRAFANA_PORT:-4500}
GRAFANA_ADMIN_PASSWORD=${GRAFANA_PASSWORD:-admin}

GRAFANA_STARTUP_URL="http://localhost:$GRAFANA_PORT/d/1rsE5yoGz/buildbuddy-metrics?orgId=1&refresh=5s"
DASHBOARD_URL="http://admin:$GRAFANA_ADMIN_PASSWORD@localhost:$GRAFANA_PORT/api/dashboards/db/buildbuddy-metrics"
DASHBOARD_FILE_PATH="./grafana/dashboards/buildbuddy.json"

# Open Grafana dashboard when the server is up and running
(
  open=$(which open &>/dev/null && echo "open" || echo "xdg-open")
  tries=100
  while ! curl "$GRAFANA_STARTUP_URL" &>/dev/null ; do
    sleep 0.5
    tries=$(( tries - 1 ))
    if [[ $tries == 0 ]] ; then
      exit 1
    fi
  done
  "$open" "$GRAFANA_STARTUP_URL"
) &

function sync () {
  local json=$(curl "$DASHBOARD_URL" 2>/dev/null)
  if [[ -z "$json" ]] ; then
    echo "$0: WARNING: Could not download dashboard from $DASHBOARD_URL"
    return
  fi

  json=$(echo "$json" | jq -r "$dashboard")
  current=$(cat "$DASHBOARD_FILE_PATH")
  # If the dashboard hasn't changed, don't write a new JSON file, to avoid
  # updating the file timestamp (causing Grafana to show "someone else updated
  # this dashboard")
  if [ "$json" == "$current" ] ; then return; fi
  echo "Detected change in Grafana dashboard. Saving to $DASHBOARD_FILE_PATH"
  echo "$json" > "$DASHBOARD_FILE_PATH"
}

# Poll for dashboard changes and update the local JSON files.
(
  while true ; do
    sleep 5
    sync
  done
) &

# Run Grafana and Prometheus
docker-compose up
