<!--
{
  "name": "Prometheus Metrics",
  "category": "5fcfd1ede5ded705a0bf5fd0",
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

## Invocation log uploads

All invocation metrics are recorded at the _end_ of each invocation.

### **`buildbuddy_invocation_count`** (Counter)

The total number of invocations whose logs were uploaded to BuildBuddy.

#### Labels

- **invocation_status**: Invocation status: `success`, `failure`, `disconnected`, or `unknown`.

#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Number of invocations per second by invocation status</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f8f8f2"> </span><span style="color: #66d9ef">by</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #f8f8f2">invocation_status</span><span style="color: #f92672">)</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_invocation_count[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br><br><span style="color: #75715e"># Invocation success rate</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_invocation_count{invocation_status</span><span style="color: #f92672">=</span><span style="color: #f8f8f2">&quot;</span><span style="color: #e6db74">success</span><span style="color: #f8f8f2">&quot;}[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br><span style="color: #f8f8f2">  </span><span style="color: #f92672">/</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_invocation_count[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br></pre></div>


### **`buildbuddy_invocation_duration_usec`** (Histogram)

The total duration of each invocation, in **microseconds**.

#### Labels

- **invocation_status**: Invocation status: `success`, `failure`, `disconnected`, or `unknown`.

#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Median invocation duration in the past 5 minutes</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">histogram_quantile</span><span style="color: #f92672">(</span><span style="color: #f8f8f2"></span><br><span style="color: #f8f8f2">  </span><span style="color: #ae81ff">0.5</span><span style="color: #f8f8f2">,</span><br><span style="color: #f8f8f2">  </span><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_invocation_duration_usec_bucket[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"> </span><span style="color: #66d9ef">by</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #f8f8f2">le</span><span style="color: #f92672">)</span><span style="color: #f8f8f2"></span><br><span style="color: #f92672">)</span><span style="color: #f8f8f2"></span><br></pre></div>


### **`buildbuddy_invocation_build_event_count`** (Counter)

Number of [build events](https://docs.bazel.build/versions/master/build-event-protocol.html) uploaded to BuildBuddy.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).

#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Build events uploaded per second</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_invocation_build_event_count[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br><br><span style="color: #75715e"># Approximate error rate of build event upload handler</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_invocation_build_event_count{status</span><span style="color: #f92672">=</span><span style="color: #f8f8f2">&quot;</span><span style="color: #e6db74">0</span><span style="color: #f8f8f2">&quot;}[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br><span style="color: #f8f8f2">  </span><span style="color: #f92672">/</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_invocation_build_event_count[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br></pre></div>

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

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Cache download rate (bytes per second)</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_cache_download_size_bytes_sum[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br></pre></div>


### **`buildbuddy_remote_cache_download_duration_usec`** (Histogram)

Download duration for each file downloaded from the remote cache, in **microseconds**.

#### Labels

- **cache_type**: Cache type: `action` for action cache, `cas` for content-addressable storage.

#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Median download duration for content-addressable store (CAS)</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">histogram_quantile</span><span style="color: #f92672">(</span><span style="color: #f8f8f2"></span><br><span style="color: #f8f8f2">  </span><span style="color: #ae81ff">0.5</span><span style="color: #f8f8f2">,</span><br><span style="color: #f8f8f2">  </span><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_remote_cache_download_duration_usec{cache_type</span><span style="color: #f92672">=</span><span style="color: #f8f8f2">&quot;</span><span style="color: #e6db74">cas</span><span style="color: #f8f8f2">&quot;}[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"> </span><span style="color: #66d9ef">by</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #f8f8f2">le</span><span style="color: #f92672">)</span><span style="color: #f8f8f2"></span><br><span style="color: #f92672">)</span><span style="color: #f8f8f2"></span><br></pre></div>


### **`buildbuddy_remote_cache_upload_size_bytes`** (Histogram)

Number of bytes uploaded to the remote cache in each upload.

Use the **`_sum`** suffix to get the total uploaded bytes and the **`_count`** suffix to get the number of uploaded files.

#### Labels

- **cache_type**: Cache type: `action` for action cache, `cas` for content-addressable storage.

#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Cache upload rate (bytes per second)</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_cache_upload_size_bytes_sum[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br></pre></div>


### **`buildbuddy_remote_cache_upload_duration_usec`** (Histogram)

Upload duration for each file uploaded to the remote cache, in **microseconds**.

#### Labels

- **cache_type**: Cache type: `action` for action cache, `cas` for content-addressable storage.

#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Median upload duration for content-addressable store (CAS)</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">histogram_quantile</span><span style="color: #f92672">(</span><span style="color: #f8f8f2"></span><br><span style="color: #f8f8f2">  </span><span style="color: #ae81ff">0.5</span><span style="color: #f8f8f2">,</span><br><span style="color: #f8f8f2">  </span><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_remote_cache_upload_duration_usec{cache_type</span><span style="color: #f92672">=</span><span style="color: #f8f8f2">&quot;</span><span style="color: #e6db74">cas</span><span style="color: #f8f8f2">&quot;}[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"> </span><span style="color: #66d9ef">by</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #f8f8f2">le</span><span style="color: #f92672">)</span><span style="color: #f8f8f2"></span><br><span style="color: #f92672">)</span><span style="color: #f8f8f2"></span><br></pre></div>

## Remote execution metrics

### **`buildbuddy_remote_execution_count`** (Counter)

Number of actions executed remotely.

#### Labels

- **exit_code**: Process exit code of an executed action.

#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Total number of actions executed per second</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_remote_execution_count[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br></pre></div>


### **`buildbuddy_remote_execution_queue_length`** (Gauge)

Number of actions currently waiting in the executor queue.
#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Median queue length across all executors</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">quantile</span><span style="color: #f92672">(</span><span style="color: #ae81ff">0.5</span><span style="color: #f8f8f2">, buildbuddy_remote_execution_queue_length</span><span style="color: #f92672">)</span><span style="color: #f8f8f2"></span><br></pre></div>


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

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Bytes downloaded per second</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_blobstore_read_size_bytes[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br></pre></div>


### **`buildbuddy_blobstore_read_duration_usec`** (Histogram)

Duration per blobstore file read, in **microseconds**.

#### Labels

- **blobstore_type**: `gcs` (Google Cloud Storage), `aws_s3`, or `disk`.


### **`buildbuddy_blobstore_write_count`** (Counter)

Number of files written to the blobstore.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
- **blobstore_type**: `gcs` (Google Cloud Storage), `aws_s3`, or `disk`.

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Bytes uploaded per second</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_blobstore_write_size_bytes[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br></pre></div>


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

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># SQL queries per second (by query template).</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f8f8f2"> </span><span style="color: #66d9ef">by</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #f8f8f2">sql_query_template</span><span style="color: #f92672">)</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_sql_query_count[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br></pre></div>


### **`buildbuddy_sql_query_duration_usec`** (Histogram)

SQL query duration, in **microseconds**.

#### Labels

- **sql_query_template**: SQL query before substituting template parameters.

#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Median SQL query duration</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">histogram_quantile</span><span style="color: #f92672">(</span><span style="color: #f8f8f2"></span><br><span style="color: #f8f8f2">  </span><span style="color: #ae81ff">0.5</span><span style="color: #f8f8f2">,</span><br><span style="color: #f8f8f2">  </span><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_sql_query_duration_usec_bucket[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"> </span><span style="color: #66d9ef">by</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #f8f8f2">le</span><span style="color: #f92672">)</span><span style="color: #f8f8f2"></span><br><span style="color: #f92672">)</span><span style="color: #f8f8f2"></span><br></pre></div>


### **`buildbuddy_sql_error_count`** (Counter)

Number of SQL queries that resulted in an error.
#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># SQL error rate</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_sql_error_count[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br><span style="color: #f8f8f2">  </span><span style="color: #f92672">/</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_sql_query_count[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br></pre></div>

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

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Requests per second, by status code</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f8f8f2"> </span><span style="color: #66d9ef">by</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #f8f8f2">code</span><span style="color: #f92672">)</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_http_request_count[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br><br><span style="color: #75715e"># 5xx error ratio</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_http_request_count{code</span><span style="color: #f92672">=~</span><span style="color: #f8f8f2">&quot;</span><span style="color: #e6db74">5..</span><span style="color: #f8f8f2">&quot;}[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br><span style="color: #f8f8f2">  </span><span style="color: #f92672">/</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">sum</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_http_request_count[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br></pre></div>


### **`buildbuddy_http_request_handler_duration_usec`** (Histogram)

Time taken to handle each HTTP request in **microseconds**.

#### Labels

- **route**: HTTP route before substituting path parameters (`/invocation/:id`, `/settings`, ...)
- **method**: HTTP method: `GET`, `POST`, ...
- **code**: HTTP response code: `200`, `302`, `401`, `404`, `500`, ...

#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Median request duration for successfuly processed (2xx) requests.</span><span style="color: #f8f8f2"></span><br><span style="color: #75715e"># Other status codes may be associated with early-exits and are</span><span style="color: #f8f8f2"></span><br><span style="color: #75715e"># likely to add too much noise.</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">histogram_quantile</span><span style="color: #f92672">(</span><span style="color: #f8f8f2"></span><br><span style="color: #f8f8f2">  </span><span style="color: #ae81ff">0.5</span><span style="color: #f8f8f2">,</span><br><span style="color: #f8f8f2">  </span><span style="color: #66d9ef">sum</span><span style="color: #f8f8f2"> </span><span style="color: #66d9ef">by</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #f8f8f2">le</span><span style="color: #f92672">)</span><span style="color: #f8f8f2">	</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_http_request_handler_duration_usec{code</span><span style="color: #f92672">=~</span><span style="color: #f8f8f2">&quot;</span><span style="color: #e6db74">2..</span><span style="color: #f8f8f2">&quot;}[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br><span style="color: #f92672">)</span><span style="color: #f8f8f2"></span><br></pre></div>


### **`buildbuddy_http_response_size_bytes`** (Histogram)

Response size of each HTTP response in **bytes**.

#### Labels

- **route**: HTTP route before substituting path parameters (`/invocation/:id`, `/settings`, ...)
- **method**: HTTP method: `GET`, `POST`, ...
- **code**: HTTP response code: `200`, `302`, `401`, `404`, `500`, ...

#### Examples

<div class="highlight" style="background: #272822"><pre style="line-height: 125%;"><span></span><span style="color: #75715e"># Median HTTP response size</span><span style="color: #f8f8f2"></span><br><span style="color: #66d9ef">histogram_quantile</span><span style="color: #f92672">(</span><span style="color: #f8f8f2"></span><br><span style="color: #f8f8f2">  </span><span style="color: #ae81ff">0.5</span><span style="color: #f8f8f2">,</span><br><span style="color: #f8f8f2">  </span><span style="color: #66d9ef">sum</span><span style="color: #f8f8f2"> </span><span style="color: #66d9ef">by</span><span style="color: #f8f8f2"> </span><span style="color: #f92672">(</span><span style="color: #f8f8f2">le</span><span style="color: #f92672">)</span><span style="color: #f8f8f2">	</span><span style="color: #f92672">(</span><span style="color: #66d9ef">rate</span><span style="color: #f92672">(</span><span style="color: #f8f8f2">buildbuddy_http_response_size_bytes[</span><span style="color: #e6db74">5m</span><span style="color: #f8f8f2">]</span><span style="color: #f92672">))</span><span style="color: #f8f8f2"></span><br><span style="color: #f92672">)</span><span style="color: #f8f8f2"></span><br></pre></div>

## Internal metrics

These metrics are for monitoring lower-level subsystems of BuildBuddy.

### Build event handler

The build event handler logs all build events uploaded to BuildBuddy
as part of the Build Event Protocol.

### **`buildbuddy_build_event_handler_duration_usec`** (Histogram)

The time spent handling each build event in **microseconds**.

#### Labels

- **status**: Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).

