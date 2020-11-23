<!--
{
  "name": "Metrics Guide",
  "category": "5f84be4816a46768724ca126",
  "priority": 1000
}
-->

<!--

============================
GENERATED FILE - DO NOT EDIT
============================

Run `python3 server/metrics/generate_docs.py` to re-generate.

-->

# BuildBuddy metrics

BuildBuddy exposes [Prometheus](https://prometheus.io) metrics that allow monitoring the
[four golden signals](https://landing.google.com/sre/sre-book/chapters/monitoring-distributed-systems/):
latency, traffic, errors, and saturation.

To view these metrics in a live-updating dashboard, we recommend using a tool
like [Grafana](https://grafana.com).

## Build Event Handler

These metrics are for monitoring BuildBuddy's handling of build events.
### **`buildbuddy_build_event_handler_duration`** (Histogram)

The time spent handling each build event in **microseconds**.

Use the **`_count`** suffix to get the total number of build events handled.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).

## Action Cache

These metrics are available when using BuildBuddy's remote cache.
### **`buildbuddy_action_cache_duration`** (Histogram)

The time spent handling each action cache request in **microseconds**.

Use the **`_count`** suffix to get the total number of action cache requests handled.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
- **event_type**: Cache event type: `hit`, `miss`, or `upload`.

