# BuildBuddy metrics tools

This directory contains tools to develop Prometheus metrics for BuildBuddy.

## Pre-requisites

To use these metrics tools, you'll need to install the following on your
machine:

- `docker-compose`
- `gcloud` (if you want to view metrics from a Kubernetes cluster)

## View metrics from your local BuildBuddy server in a local Grafana instance

1. Run `./run.sh` in this directory to start Prometheus and Grafana locally
   (using Docker).
2. Visit `http://localhost:4500` if it doesn't open automatically.
3. Log in with the username and password `admin`.
4. If the BuildBuddy dashboard isn't already shown, navigate to the
   "BuildBuddy metrics" dashboard.

### View metrics from a Kubernetes cluster in a local Grafana instance

Follow the same steps as above, using the `kube` argument: `./run.sh kube`.
This will point Grafana at Prometheus running in Kubernetes (using port
forwarding) instead of running Prometheus locally.

The following env variables can be provided to this command:

- `KUBE_NAMESPACE`: the Kubernetes namespace to use. (Default: `monitor-dev`).
- `KUBE_PROM_SERVER_RESOURCE`: prometheus server resource name. Follows
  the same `TYPE/NAME` format expected by the `kubectl port-forward`
  command. (Default: `deployment/prometheus-server`).
- `KUBE_PROM_SERVER_PORT`: prometheus server port. (Default: `9090`).
