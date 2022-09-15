#!/bin/bash

set -eo pipefail
__dir__=$(dirname "$0")

cd "$__dir__"

START_BRANCH=$(git branch --show-current)

: ${GRAFANA_PORT:=4500}
: ${GRAFANA_ADMIN_PASSWORD:="admin"}
: ${GRAFANA_DASHBOARD_ID:=1rsE5yoGz}
GRAFANA_STARTUP_URL="http://localhost:$GRAFANA_PORT/d/$GRAFANA_DASHBOARD_ID?orgId=1&refresh=5s"
GRAFANA_DASHBOARD_URL="http://admin:$GRAFANA_ADMIN_PASSWORD@localhost:$GRAFANA_PORT/api/dashboards/uid/$GRAFANA_DASHBOARD_ID"
GRAFANA_DASHBOARD_FILE_PATH="./grafana/dashboards/buildbuddy.json"

: ${KUBE_CONTEXT:=""}
: ${KUBE_NAMESPACE:="monitor-dev"}
: ${KUBE_PROM_SERVER_RESOURCE:="deployment/prometheus-global-server"}
: ${KUBE_PROM_SERVER_PORT:=9090}

# Open Grafana dashboard when the server is up and running
(
  open=$(which open &>/dev/null && echo "open" || echo "xdg-open")
  tries=100
  while ! (curl "$GRAFANA_STARTUP_URL" &>/dev/null && curl "http://localhost:9100/metrics" &>/dev/null); do
    sleep 0.5
    tries=$((tries - 1))
    if [[ $tries == 0 ]]; then
      exit 1
    fi
  done
  echo "Opening $GRAFANA_STARTUP_URL"
  "$open" "$GRAFANA_STARTUP_URL"
) &

function sync() {
  local json
  json=$(curl "$GRAFANA_DASHBOARD_URL" 2>/dev/null)
  if [[ -z "$json" ]]; then
    echo "$0: WARNING: Could not download dashboard from $GRAFANA_DASHBOARD_URL"
    return
  fi

  json=$(echo "$json" | ./process_dashboard.py)
  if [[ -z "$json" ]]; then return 1; fi
  current=$(cat "$GRAFANA_DASHBOARD_FILE_PATH")
  # If the dashboard hasn't changed, don't write a new JSON file, to avoid
  # updating the file timestamp (causing Grafana to show "someone else updated
  # this dashboard")
  if [ "$json" == "$current" ]; then return; fi

  local current_branch
  current_branch=$(git branch --show-current)
  if [[ "$current_branch" != "$START_BRANCH" ]]; then
    echo -e "$0: \033[33mWARNING: git branch has changed. Changes to the dashboard will not be auto-saved.\033[0m"
    return
  fi

  echo "$0: Detected change in Grafana dashboard. Saving to $GRAFANA_DASHBOARD_FILE_PATH"
  echo "$json" >"$GRAFANA_DASHBOARD_FILE_PATH"
}

# Poll for dashboard changes and update the local JSON files.
(
  while true; do
    sleep 3
    sync || true
  done
) &

docker_compose_args=(
  "-f" "docker-compose.grafana.yml"
  "-f" "docker-compose.redis-exporter.yml"
)
if [[ "$1" == "kube" ]]; then
  # Start a thread to forward port 9100 locally to the Prometheus server on Kube.
  (
    kubectl --context="$KUBE_CONTEXT" --namespace="$KUBE_NAMESPACE" \
      port-forward "$KUBE_PROM_SERVER_RESOURCE" --address 0.0.0.0 9100:"$KUBE_PROM_SERVER_PORT"
  ) &
else
  # Run the Prometheus server locally.
  docker_compose_args+=("-f" "docker-compose.prometheus.yml")
fi

docker-compose "${docker_compose_args[@]}" up
