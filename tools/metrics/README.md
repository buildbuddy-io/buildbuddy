# BuildBuddy metrics tools

This directory contains tools to develop Prometheus metrics for BuildBuddy.

## Pre-requisites

To use these metrics tools, you'll need to install the following on your
machine:

- `docker-compose`
- `kubectl` (if you want to point Grafana at a kubernetes cluster).

## View metrics from your local BuildBuddy server in a local Grafana instance

1. Run `bazel run tools/metrics/grafana` to start Grafana and Prometheus
   locally (using `docker-compose`).
2. Visit http://localhost:4500 if it doesn't open automatically.
3. If the BuildBuddy dashboard isn't already shown, navigate to the
   "BuildBuddy metrics" dashboard.

## Saving dashboard changes

To save dashboard changes, press the save icon in the Grafana UI.
Changes will not be synced automatically!

## View metrics from a Kubernetes cluster in a local Grafana instance

Switch to your desired context with `kubectl config set-context`, then
run:

```
bazel run tools/metrics/grafana -- -kube -namespace=monitor-{dev,prod}
```

This will point Grafana at Prometheus running in Kubernetes (using port
forwarding) instead of running Prometheus locally.
