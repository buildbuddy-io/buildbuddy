# BuildBuddy metrics tools

This directory contains tools to develop Prometheus metrics for BuildBuddy.

## Getting started

1. Install `docker-compose`.
2. Run `./run.sh` in this directory to start Prometheus and Grafana (using Docker).
3. Visit `http://localhost:4500`.
4. Log in with the username and password `admin`.

## Viewing metrics in Grafana

To view BuildBuddy metrics in Grafana:

1. In Grafana, create a new dashboard.
2. In the `Metric` dropdown in the UI, check out the `buildbuddy` sub-menu.
3. Select one of the metrics to view it in the graph.
4. Run some tests against your local BuildBuddy server and make sure the metric
   is updated as you'd expect.
