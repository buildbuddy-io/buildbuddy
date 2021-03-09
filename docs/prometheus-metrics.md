---
id: prometheus-metrics
title: Prometheus Metrics
sidebar_label: Prometheus Metrics
---

<!--

============================
GENERATED FILE - DO NOT EDIT
============================

Run `python3 server/metrics/generate_docs.py` to re-generate.

-->

BuildBuddy exposes [Prometheus](https://prometheus.io) metrics that allow monitoring the
[four golden signals](https://landing.google.com/sre/sre-book/chapters/monitoring-distributed-systems/):
latency, traffic, errors, and saturation.

To view these metrics in a live-updating dashboard, we recommend using a tool
like [Grafana](https://grafana.com).

## Invocation log uploads

All invocation metrics are recorded at the _end_ of each invocation.

### **`buildbuddy_invocation_count`** (Counter)

The total number of invocations whose logs were uploaded to BuildBuddy.

#### Labels

- **invocation_status**: Invocation status: `success`, `failure`, `disconnected`, or `unknown`.

#### Examples

```promql
# Number of invocations per second by invocation status
sum by (invocation_status) (rate(buildbuddy_invocation_count[5m]))

# Invocation success rate
sum(rate(buildbuddy_invocation_count{invocation_status="success"}[5m]))
  /
sum(rate(buildbuddy_invocation_count[5m]))
```

### **`buildbuddy_invocation_duration_usec`** (Histogram)

The total duration of each invocation, in **microseconds**.

#### Labels

- **invocation_status**: Invocation status: `success`, `failure`, `disconnected`, or `unknown`.

#### Examples

```promql
# Median invocation duration in the past 5 minutes
histogram_quantile(
  0.5,
  sum(rate(buildbuddy_invocation_duration_usec_bucket[5m])) by (le)
)
```

### **`buildbuddy_invocation_build_event_count`** (Counter)

Number of [build events](https://docs.bazel.build/versions/master/build-event-protocol.html) uploaded to BuildBuddy.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).

#### Examples

```promql
# Build events uploaded per second
sum(rate(buildbuddy_invocation_build_event_count[5m]))

# Approximate error rate of build event upload handler
sum(rate(buildbuddy_invocation_build_event_count{status="0"}[5m]))
  /
sum(rate(buildbuddy_invocation_build_event_count[5m]))
```
## Remote cache metrics

NOTE: Cache metrics are recorded at the end of each invocation,
which means that these metrics provide _approximate_ real-time signals.

### **`buildbuddy_remote_cache_events`** (Counter)

Number of cache events handled.

#### Labels

- **cache_type**: Cache type: `action` for action cache, `cas` for content-addressable storage.
- **cache_event_type**: Cache event type: `hit`, `miss`, or `upload`.


### **`buildbuddy_remote_cache_download_size_bytes`** (Histogram)

Number of bytes downloaded from the remote cache in each download.

Use the **`_sum`** suffix to get the total downloaded bytes and the **`_count`** suffix to get the number of downloaded files.

#### Labels

- **cache_type**: Cache type: `action` for action cache, `cas` for content-addressable storage.

#### Examples

```promql
# Cache download rate (bytes per second)
sum(rate(buildbuddy_cache_download_size_bytes_sum[5m]))
```

### **`buildbuddy_remote_cache_download_duration_usec`** (Histogram)

Download duration for each file downloaded from the remote cache, in **microseconds**.

#### Labels

- **cache_type**: Cache type: `action` for action cache, `cas` for content-addressable storage.

#### Examples

```promql
# Median download duration for content-addressable store (CAS)
histogram_quantile(
  0.5,
  sum(rate(buildbuddy_remote_cache_download_duration_usec{cache_type="cas"}[5m])) by (le)
)
```

### **`buildbuddy_remote_cache_upload_size_bytes`** (Histogram)

Number of bytes uploaded to the remote cache in each upload.

Use the **`_sum`** suffix to get the total uploaded bytes and the **`_count`** suffix to get the number of uploaded files.

#### Labels

- **cache_type**: Cache type: `action` for action cache, `cas` for content-addressable storage.

#### Examples

```promql
# Cache upload rate (bytes per second)
sum(rate(buildbuddy_cache_upload_size_bytes_sum[5m]))
```

### **`buildbuddy_remote_cache_upload_duration_usec`** (Histogram)

Upload duration for each file uploaded to the remote cache, in **microseconds**.

#### Labels

- **cache_type**: Cache type: `action` for action cache, `cas` for content-addressable storage.

#### Examples

```promql
# Median upload duration for content-addressable store (CAS)
histogram_quantile(
  0.5,
  sum(rate(buildbuddy_remote_cache_upload_duration_usec{cache_type="cas"}[5m])) by (le)
)
```
## Remote execution metrics

### **`buildbuddy_remote_execution_count`** (Counter)

Number of actions executed remotely.

#### Labels

- **exit_code**: Process exit code of an executed action.

#### Examples

```promql
# Total number of actions executed per second
sum(rate(buildbuddy_remote_execution_count[5m]))
```

### **`buildbuddy_remote_execution_executed_action_metadata_durations_usec`** (Histogram)

Time spent in each stage of action execution, in **microseconds**.

Queries should filter or group by the `stage` label, taking care not to aggregate different stages.

#### Labels

- **stage**: Command provided to the Bazel daemon: `run`, `test`, `build`, `coverage`, `mobile-install`, ... Executed action stage. Action execution is split into stages corresponding to the timestamps defined in [`ExecutedActionMetadata`](https://github.com/buildbuddy-io/buildbuddy/blob/fb2e3a74083d82797926654409dc3858089d260b/proto/remote_execution.proto#L797): `queued`, `input_fetch`, `execution`, and `output_upload`. An additional stage, `worker`, includes all stages during which a worker is handling the action, which is all stages except the `queued` stage.

#### Examples

```promql
# Median duration of all command stages
histogram_quantile(
  0.5,
  sum(rate(buildbuddy_remote_execution_executed_action_metadata_durations_usec_bucket[5m])) by (le, stage)
)

# p90 duration of just the command execution stage
histogram_quantile(
  0.9,
  sum(rate(buildbuddy_remote_execution_executed_action_metadata_durations_usec_bucket{stage="execution"}[5m])) by (le)
)
```

### **`buildbuddy_remote_execution_queue_length`** (Gauge)

Number of actions currently waiting in the executor queue.
#### Examples

```promql
# Median queue length across all executors
quantile(0.5, buildbuddy_remote_execution_queue_length)
```

### **`buildbuddy_remote_execution_tasks_executing`** (Gauge)

Number of tasks currently being executed by the executor.
#### Examples

```promql
# Fraction of idle executors
count_values(0, buildbuddy_remote_execution_tasks_executing)
  /
count(buildbuddy_remote_execution_tasks_executing)
```

### **`buildbuddy_remote_execution_assigned_ram_bytes`** (Gauge)

Estimated RAM on the executor that is currently allocated for task execution, in **bytes**.

### **`buildbuddy_remote_execution_assigned_milli_cpu`** (Gauge)

Estimated CPU time on the executor that is currently allocated for task execution, in Kubernetes milliCPU.
#### Examples

```promql
# Average CPU allocated to tasks (average is computed across executor instances).
# `label_replace` is needed because we export k8s pod name as "pod_name" in Prometheus,
# while k8s exports it as "pod".
avg(
  buildbuddy_remote_execution_used_milli_cpu
    /
  on (pod_name) (label_replace(
    kube_pod_container_resource_limits_cpu_cores{pod=~"executor-.*"},
    "pod_name", "$1", "pod", "(.*)"
  ) * 1000 * 0.6)
)
```

### **`buildbuddy_remote_execution_file_download_count`** (Histogram)

Number of files downloaded during remote execution.

### **`buildbuddy_remote_execution_file_download_size_bytes`** (Histogram)

Total number of bytes downloaded during remote execution.

### **`buildbuddy_remote_execution_file_download_duration_usec`** (Histogram)

Per-file download duration during remote execution, in **microseconds**.

### **`buildbuddy_remote_execution_file_upload_count`** (Histogram)

Number of files uploaded during remote execution.

### **`buildbuddy_remote_execution_file_upload_size_bytes`** (Histogram)

Total number of bytes uploaded during remote execution.

### **`buildbuddy_remote_execution_file_upload_duration_usec`** (Histogram)

Per-file upload duration during remote execution, in **microseconds**.
## Blobstore metrics

"Blobstore" refers to the backing storage that BuildBuddy uses to
store objects in the cache, as well as certain pieces of temporary
data (such as invocation events while an invocation is in progress).

### **`buildbuddy_blobstore_read_count`** (Counter)

Number of files read from the blobstore.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
- **blobstore_type**: `gcs` (Google Cloud Storage), `aws_s3`, or `disk`.


### **`buildbuddy_blobstore_read_size_bytes`** (Histogram)

Number of bytes read from the blobstore per file.

#### Labels

- **blobstore_type**: `gcs` (Google Cloud Storage), `aws_s3`, or `disk`.

```promql
# Bytes downloaded per second
sum(rate(buildbuddy_blobstore_read_size_bytes[5m]))
```

### **`buildbuddy_blobstore_read_duration_usec`** (Histogram)

Duration per blobstore file read, in **microseconds**.

#### Labels

- **blobstore_type**: `gcs` (Google Cloud Storage), `aws_s3`, or `disk`.


### **`buildbuddy_blobstore_write_count`** (Counter)

Number of files written to the blobstore.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
- **blobstore_type**: `gcs` (Google Cloud Storage), `aws_s3`, or `disk`.

```promql
# Bytes uploaded per second
sum(rate(buildbuddy_blobstore_write_size_bytes[5m]))
```

### **`buildbuddy_blobstore_write_size_bytes`** (Histogram)

Number of bytes written to the blobstore per file.

#### Labels

- **blobstore_type**: `gcs` (Google Cloud Storage), `aws_s3`, or `disk`.


### **`buildbuddy_blobstore_write_duration_usec`** (Histogram)

Duration per blobstore file write, in **microseconds**.

#### Labels

- **blobstore_type**: `gcs` (Google Cloud Storage), `aws_s3`, or `disk`.


### **`buildbuddy_blobstore_delete_count`** (Counter)

Number of files deleted from the blobstore.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
- **blobstore_type**: `gcs` (Google Cloud Storage), `aws_s3`, or `disk`.


### **`buildbuddy_blobstore_delete_duration_usec`** (Histogram)

Delete duration per blobstore file deletion, in **microseconds**.

#### Labels

- **blobstore_type**: `gcs` (Google Cloud Storage), `aws_s3`, or `disk`.

# SQL metrics

The following metrics are for monitoring the SQL database configured
for BuildBuddy.

If you'd like to see an up-to-date catalog of what BuildBuddy stores in
its SQL database, see the table definitions [here](https://github.com/buildbuddy-io/buildbuddy/blob/master/server/tables/tables.go).

## Query / error rate metrics

### **`buildbuddy_sql_query_count`** (Counter)

Number of SQL queries executed.

#### Labels

- **sql_query_template**: SQL query before substituting template parameters.

#### Examples

```promql
# SQL queries per second (by query template).
sum by (sql_query_template) (rate(buildbuddy_sql_query_count[5m]))
```

### **`buildbuddy_sql_query_duration_usec`** (Histogram)

SQL query duration, in **microseconds**.

#### Labels

- **sql_query_template**: SQL query before substituting template parameters.

#### Examples

```promql
# Median SQL query duration
histogram_quantile(
  0.5,
  sum(rate(buildbuddy_sql_query_duration_usec_bucket[5m])) by (le)
)
```

### **`buildbuddy_sql_error_count`** (Counter)

Number of SQL queries that resulted in an error.

#### Labels

- **sql_query_template**: SQL query before substituting template parameters.

#### Examples

```promql
# SQL error rate
sum(rate(buildbuddy_sql_error_count[5m]))
  /
sum(rate(buildbuddy_sql_query_count[5m]))
```
## `database/sql` metrics

The following metrics directly expose
[DBStats](https://golang.org/pkg/database/sql/#DBStats) from the
`database/sql` Go package.

### **`buildbuddy_sql_max_open_connections`** (Gauge)

Maximum number of open connections to the database.

#### Labels

- **sql_db_role**: SQL DB replica role: `primary` for read+write replicas, or `read_replica` for read-only DB replicas.


### **`buildbuddy_sql_open_connections`** (Gauge)

The number of established connections to the database.

#### Labels

- **connection_status**: Status of the database connection: `in_use` or `idle`
- **sql_db_role**: SQL DB replica role: `primary` for read+write replicas, or `read_replica` for read-only DB replicas.


### **`buildbuddy_sql_wait_count`** (Counter)

The total number of connections waited for.

#### Labels

- **sql_db_role**: SQL DB replica role: `primary` for read+write replicas, or `read_replica` for read-only DB replicas.


### **`buildbuddy_sql_wait_duration_usec`** (Counter)

The total time blocked waiting for a new connection, in **microseconds**.

#### Labels

- **sql_db_role**: SQL DB replica role: `primary` for read+write replicas, or `read_replica` for read-only DB replicas.


### **`buildbuddy_sql_max_idle_closed`** (Counter)

The total number of connections closed due to SetMaxIdleConns.

#### Labels

- **sql_db_role**: SQL DB replica role: `primary` for read+write replicas, or `read_replica` for read-only DB replicas.


### **`buildbuddy_sql_max_idle_time_closed`** (Counter)

The total number of connections closed due to SetConnMaxIdleTime.

#### Labels

- **sql_db_role**: SQL DB replica role: `primary` for read+write replicas, or `read_replica` for read-only DB replicas.


### **`buildbuddy_sql_max_lifetime_closed`** (Counter)

The total number of connections closed due to SetConnMaxLifetime.

#### Labels

- **sql_db_role**: SQL DB replica role: `primary` for read+write replicas, or `read_replica` for read-only DB replicas.

## HTTP metrics

### **`buildbuddy_http_request_count`** (Counter)

HTTP request count.

#### Labels

- **route**: HTTP route before substituting path parameters (`/invocation/:id`, `/settings`, ...)
- **method**: HTTP method: `GET`, `POST`, ...

#### Examples

```promql
# Requests per second, by status code
sum by (code) (rate(buildbuddy_http_request_count[5m]))

# 5xx error ratio
sum(rate(buildbuddy_http_request_count{code=~"5.."}[5m]))
  /
sum(rate(buildbuddy_http_request_count[5m]))
```

### **`buildbuddy_http_request_handler_duration_usec`** (Histogram)

Time taken to handle each HTTP request in **microseconds**.

#### Labels

- **route**: HTTP route before substituting path parameters (`/invocation/:id`, `/settings`, ...)
- **method**: HTTP method: `GET`, `POST`, ...
- **code**: HTTP response code: `200`, `302`, `401`, `404`, `500`, ...

#### Examples

```promql
# Median request duration for successfuly processed (2xx) requests.
# Other status codes may be associated with early-exits and are
# likely to add too much noise.
histogram_quantile(
  0.5,
  sum by (le)	(rate(buildbuddy_http_request_handler_duration_usec{code=~"2.."}[5m]))
)
```

### **`buildbuddy_http_response_size_bytes`** (Histogram)

Response size of each HTTP response in **bytes**.

#### Labels

- **route**: HTTP route before substituting path parameters (`/invocation/:id`, `/settings`, ...)
- **method**: HTTP method: `GET`, `POST`, ...
- **code**: HTTP response code: `200`, `302`, `401`, `404`, `500`, ...

#### Examples

```promql
# Median HTTP response size
histogram_quantile(
  0.5,
  sum by (le)	(rate(buildbuddy_http_response_size_bytes[5m]))
)
```
## Internal metrics

These metrics are for monitoring lower-level subsystems of BuildBuddy.

### Build event handler

The build event handler logs all build events uploaded to BuildBuddy
as part of the Build Event Protocol.

### **`buildbuddy_build_event_handler_duration_usec`** (Histogram)

The time spent handling each build event in **microseconds**.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).

### Cache

"Cache" refers to the cache backend(s) that BuildBuddy uses to
accelerate file IO operations, which are common in different
subsystems such as the remote cache and the fetch server (for
downloading invocation artifacts).

BuildBuddy can be configured to use multiple layers of caching
(an in-memory layer, coupled with a cloud storage layer).

#### `get` metrics

`get` metrics track non-streamed cache reads (all data is fetched
from the cache in a single request).

### **`buildbuddy_cache_get_count`** (Counter)

Number of cache get requests.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_get_duration_usec`** (Histogram)

The time spent retrieving each entry from the cache, in **microseconds**.

This is recorded only for successful gets.

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_get_size_bytes`** (Histogram)

Size of each entry retrieved from the cache, in **bytes**.

This is recorded only for successful gets.

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.

#### `read` metrics

`read` metrics track streamed cache reads.

### **`buildbuddy_cache_read_count`** (Counter)

Number of streamed cache reads started.

This is incremented once for each started stream, **not** for each chunk in the stream.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_read_duration_usec`** (Histogram)

The total time spent for each read stream, in **microseconds**.

This is recorded only for successful reads, and measures the entire read stream (not just individual chunks).

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_read_size_bytes`** (Histogram)

Total size of each entry retrieved from the cache via streaming, in **bytes**.

This is recorded only on success, and measures the entire stream (not just individual chunks).

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.

#### `set` metrics

`set` metrics track non-streamed cache writes (all data is wrtiten
in a single request).

### **`buildbuddy_cache_set_count`** (Counter)

Number of cache set requests.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_set_duration_usec`** (Histogram)

The time spent writing each entry to the cache, in **microseconds**.

This is recorded only for successful sets.

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_set_size_bytes`** (Histogram)

Size of the value stored in each set operation, in **bytes**.

This is recorded only for successful sets.

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_set_retries`** (Histogram)

Number of retries required to fulfill the set request (an observed value of 0 means the transfer succeeded on the first try).

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.

#### `write` metrics

`write` metrics track streamed cache writes.

### **`buildbuddy_cache_write_count`** (Counter)

Number of streamed cache writes started.

This is incremented once for each started stream, **not** for each chunk in the stream.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_write_duration_usec`** (Histogram)

The time spent for each streamed write to the cache, in **microseconds**.

This is recorded only on success, and measures the entire stream (not just individual chunks).

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_write_size_bytes`** (Histogram)

Size of each entry written to the cache via streaming, in **bytes**.

This is recorded only on success, and measures the entire stream (not just individual chunks).

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_write_retries`** (Histogram)

Number of retries required to write each chunk in the stream (an observed value of 0 means the transfer succeeded on the first try).

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.

### Other cache metrics

### **`buildbuddy_cache_delete_count`** (Counter)

Number of deletes from the cache.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_delete_duration_usec`** (Histogram)

Duration of each cache deletion, in **microseconds**.

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_contains_count`** (Counter)

Number of `contains(key)` requests made to the cache.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_contains_duration_usec`** (Histogram)

Duration of each each `contains(key)` request, in **microseconds**.

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.


### **`buildbuddy_cache_contains_retry_count`** (Histogram)

Number of retries required to fulfill each `contains(key)` request to the cache (an observed value of 0 means the request succeeded on the first try).

#### Labels

- **tier**: Cache tier: `memory` or `cloud`. This label can be used to write Prometheus queries that don't break if the cache backend is swapped out for a different backend.
- **backend**: Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.

