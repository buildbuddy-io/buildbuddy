package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Note: the doc generator script (`generate_docs.py`) in this directory
// generates documentation from this file.
//
// The doc generator treats comments starting with 3 slashes as markdown docs,
// as well as the 'Help' field for each metric.
//
// Run `python3 generate_docs.py --watch` to interactively generate the
// docs as you edit this file.

const (
	// Label constants.
	// Commonly used labels can be added here, and their documentation will be
	// displayed in the metrics where they are used. Each constant's name should
	// end with `Label`.

	/// Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
	StatusLabel = "status"

	/// Invocation status: `success`, `failure`, `disconnected`, or `unknown`.
	InvocationStatusLabel = "invocation_status"

	/// Cache type: `action` for action cache, `cas` for content-addressable storage.
	CacheTypeLabel = "cache_type"

	// TODO(bduffany): Document the difference between `miss` and `upload`
	/// Cache event type: `hit`, `miss`, or `upload`.
	CacheEventTypeLabel = "cache_event_type"

	/// Process exit code of an executed action.
	ExitCodeLabel = "exit_code"

	/// SQL query before substituting template parameters.
	SQLQueryTemplateLabel = "sql_query_template"

	/// `gcs` (Google Cloud Storage), `aws_s3`, or `disk`.
	BlobstoreTypeLabel = "blobstore_type"

	/// Status of the database connection: `in_use` or `idle`
	SQLConnectionStatusLabel = "connection_status"

	/// SQL DB replica role: `primary` for read+write replicas, or
	/// `read_replica` for read-only DB replicas.
	SQLDBRoleLabel = "sql_db_role"

	/// HTTP route before substituting path parameters
	/// (`/invocation/:id`, `/settings`, ...)
	HTTPRouteLabel = "route"

	/// HTTP method: `GET`, `POST`, ...
	HTTPMethodLabel = "method"

	/// HTTP response code: `200`, `302`, `401`, `404`, `500`, ...
	HTTPResponseCodeLabel = "code"

	/// Cache backend: `gcs` (Google Cloud Storage), `aws_s3`, or `redis`.
	CacheBackendLabel = "backend"

	/// Cache tier: `memory` or `cloud`. This label can be used to write Prometheus
	/// queries that don't break if the cache backend is swapped out for
	/// a different backend.
	CacheTierLabel = "tier"

	/// Command provided to the Bazel daemon: `run`, `test`, `build`, `coverage`, `mobile-install`, ...
	BazelCommand = "bazel_command"

	/// Executed action stage. Action execution is split into stages corresponding to
	/// the timestamps defined in
	/// [`ExecutedActionMetadata`](https://github.com/buildbuddy-io/buildbuddy/blob/fb2e3a74083d82797926654409dc3858089d260b/proto/remote_execution.proto#L797):
	/// `queued`, `input_fetch`, `execution`, and `output_upload`. An additional stage,
	/// `worker`, includes all stages during which a worker is handling the action,
	/// which is all stages except the `queued` stage.
	ExecutedActionStageLabel = "stage"

	/// Type of event sent to BuildBuddy's webhook handler: `push` or
	/// `pull_request`.
	WebhookEventName = "event"

	/// Status of the recycle runner request: `hit` if the executor assigned a
	/// recycled runner to the action; `miss` otherwise.
	RecycleRunnerRequestStatusLabel = "status"

	/// Reason for a runner not being added to the runner pool.
	RunnerPoolFailedRecycleReason = "reason"

	/// GroupID associated with the request.
	GroupID = "group_id"

	/// EventName is the name used to identify the type of an unexpected event.
	EventName = "name"
)

const (
	bbNamespace = "buildbuddy"
)

var (
	/// ## Invocation log uploads
	///
	/// All invocation metrics are recorded at the _end_ of each invocation.

	InvocationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "count",
		Help:      "The total number of invocations whose logs were uploaded to BuildBuddy.",
	}, []string{
		InvocationStatusLabel,
		BazelCommand,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Number of invocations per second by invocation status
	/// sum by (invocation_status) (rate(buildbuddy_invocation_count[5m]))
	///
	/// # Invocation success rate
	/// sum(rate(buildbuddy_invocation_count{invocation_status="success"}[5m]))
	///   /
	/// sum(rate(buildbuddy_invocation_count[5m]))
	/// ```

	InvocationDurationUs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "The total duration of each invocation, in **microseconds**.",
	}, []string{
		InvocationStatusLabel,
		BazelCommand,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Median invocation duration in the past 5 minutes
	/// histogram_quantile(
	///   0.5,
	///   sum(rate(buildbuddy_invocation_duration_usec_bucket[5m])) by (le)
	/// )
	/// ```

	BuildEventCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "build_event_count",
		Help:      "Number of [build events](https://docs.bazel.build/versions/master/build-event-protocol.html) uploaded to BuildBuddy.",
	}, []string{
		StatusLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Build events uploaded per second
	/// sum(rate(buildbuddy_invocation_build_event_count[5m]))
	///
	/// # Approximate error rate of build event upload handler
	/// sum(rate(buildbuddy_invocation_build_event_count{status="0"}[5m]))
	///   /
	/// sum(rate(buildbuddy_invocation_build_event_count[5m]))
	/// ```

	/// ## Remote cache metrics
	///
	/// NOTE: Cache metrics are recorded at the end of each invocation,
	/// which means that these metrics provide _approximate_ real-time signals.

	CacheEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "events",
		Help:      "Number of cache events handled.",
	}, []string{
		CacheTypeLabel,
		CacheEventTypeLabel,
	})

	CacheDownloadSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "download_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Number of bytes downloaded from the remote cache in each download. Use the **`_sum`** suffix to get the total downloaded bytes and the **`_count`** suffix to get the number of downloaded files.",
	}, []string{
		CacheTypeLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Cache download rate (bytes per second)
	/// sum(rate(buildbuddy_cache_download_size_bytes_sum[5m]))
	/// ```

	CacheDownloadDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "download_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Download duration for each file downloaded from the remote cache, in **microseconds**.",
	}, []string{
		CacheTypeLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Median download duration for content-addressable store (CAS)
	/// histogram_quantile(
	///   0.5,
	///   sum(rate(buildbuddy_remote_cache_download_duration_usec{cache_type="cas"}[5m])) by (le)
	/// )
	/// ```

	CacheUploadSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "upload_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Number of bytes uploaded to the remote cache in each upload. Use the **`_sum`** suffix to get the total uploaded bytes and the **`_count`** suffix to get the number of uploaded files.",
	}, []string{
		CacheTypeLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Cache upload rate (bytes per second)
	/// sum(rate(buildbuddy_cache_upload_size_bytes_sum[5m]))
	/// ```

	CacheUploadDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "upload_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Upload duration for each file uploaded to the remote cache, in **microseconds**.",
	}, []string{
		CacheTypeLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Median upload duration for content-addressable store (CAS)
	/// histogram_quantile(
	///   0.5,
	///   sum(rate(buildbuddy_remote_cache_upload_duration_usec{cache_type="cas"}[5m])) by (le)
	/// )
	/// ```

	/// ## Remote execution metrics

	RemoteExecutionCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "count",
		Help:      "Number of actions executed remotely.",
	}, []string{
		ExitCodeLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Total number of actions executed per second
	/// sum(rate(buildbuddy_remote_execution_count[5m]))
	/// ```

	RemoteExecutionExecutedActionMetadataDurationsUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "executed_action_metadata_durations_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Time spent in each stage of action execution, in **microseconds**. Queries should filter or group by the `stage` label, taking care not to aggregate different stages.",
	}, []string{
		ExecutedActionStageLabel,
	})

	RemoteExecutionWaitingExecutionResult = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "waiting_execution_result",
		Help:      "Number of execution requests for which the client is actively waiting for results.",
	}, []string{
		GroupID,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Median duration of all command stages
	/// histogram_quantile(
	///	  0.5,
	///   sum(rate(buildbuddy_remote_execution_executed_action_metadata_durations_usec_bucket[5m])) by (le, stage)
	/// )
	///
	/// # p90 duration of just the command execution stage
	/// histogram_quantile(
	///	  0.9,
	///   sum(rate(buildbuddy_remote_execution_executed_action_metadata_durations_usec_bucket{stage="execution"}[5m])) by (le)
	/// )
	/// ```

	RemoteExecutionQueueLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "queue_length",
		Help:      "Number of actions currently waiting in the executor queue.",
	}, []string{
		GroupID,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Median queue length across all executors
	/// quantile(0.5, buildbuddy_remote_execution_queue_length)
	/// ```

	RemoteExecutionTasksExecuting = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "tasks_executing",
		Help:      "Number of tasks currently being executed by the executor.",
	})

	/// #### Examples
	///
	/// ```promql
	/// # Fraction of idle executors
	/// count_values(0, buildbuddy_remote_execution_tasks_executing)
	///   /
	/// count(buildbuddy_remote_execution_tasks_executing)
	/// ```

	RemoteExecutionAssignedRAMBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "assigned_ram_bytes",
		Help:      "Estimated RAM on the executor that is currently allocated for task execution, in **bytes**.",
	})

	RemoteExecutionAssignedMilliCPU = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "assigned_milli_cpu",
		Help:      "Estimated CPU time on the executor that is currently allocated for task execution, in Kubernetes milliCPU.",
	})

	/// #### Examples
	///
	/// ```promql
	/// # Average CPU allocated to tasks (average is computed across executor instances).
	/// # `label_replace` is needed because we export k8s pod name as "pod_name" in Prometheus,
	/// # while k8s exports it as "pod".
	/// avg(
	///   buildbuddy_remote_execution_used_milli_cpu
	///     /
	///	  on (pod_name) (label_replace(
	///     kube_pod_container_resource_limits_cpu_cores{pod=~"executor-.*"},
	///     "pod_name", "$1", "pod", "(.*)"
	///   ) * 1000 * 0.6)
	/// )
	/// ```

	FileDownloadCount = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "file_download_count",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Number of files downloaded during remote execution.",
	})

	FileDownloadSizeBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "file_download_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Total number of bytes downloaded during remote execution.",
	})

	FileDownloadDurationUsec = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "file_download_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Per-file download duration during remote execution, in **microseconds**.",
	})

	FileUploadCount = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "file_upload_count",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Number of files uploaded during remote execution.",
	})

	FileUploadSizeBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "file_upload_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Total number of bytes uploaded during remote execution.",
	})

	FileUploadDurationUsec = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "file_upload_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Per-file upload duration during remote execution, in **microseconds**.",
	})

	RecycleRunnerRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "recycle_runner_requests",
		Help:      "Number of execution requests with runner recycling enabled (via the platform property `recycle-runner=true`).",
	}, []string{
		RecycleRunnerRequestStatusLabel,
	})

	RunnerPoolCount = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "runner_pool_count",
		Help:      "Number of command runners that are currently pooled (and available for recycling).",
	})

	RunnerPoolEvictions = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "runner_pool_evictions",
		Help:      "Number of command runners removed from the pool to make room for other runners.",
	})

	RunnerPoolFailedRecycleAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "runner_pool_failed_recycle_attempts",
		Help:      "Number of failed attempts to add runners to the pool.",
	}, []string{
		RunnerPoolFailedRecycleReason,
	})

	RunnerPoolMemoryUsageBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "runner_pool_memory_usage_bytes",
		Help:      "Total memory usage of pooled command runners, in **bytes**. Currently only supported for Docker-based executors.",
	})

	RunnerPoolDiskUsageBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "runner_pool_disk_usage_bytes",
		Help:      "Total disk usage of pooled command runners, in **bytes**.",
	})

	/// ## Blobstore metrics
	///
	/// "Blobstore" refers to the backing storage that BuildBuddy uses to
	/// store objects in the cache, as well as certain pieces of temporary
	/// data (such as invocation events while an invocation is in progress).

	BlobstoreReadCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "blobstore",
		Name:      "read_count",
		Help:      "Number of files read from the blobstore.",
	}, []string{
		StatusLabel,
		BlobstoreTypeLabel,
	})

	BlobstoreReadSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "blobstore",
		Name:      "read_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Number of bytes read from the blobstore per file.",
	}, []string{
		BlobstoreTypeLabel,
	})

	/// ```promql
	/// # Bytes downloaded per second
	/// sum(rate(buildbuddy_blobstore_read_size_bytes[5m]))
	/// ```

	BlobstoreReadDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "blobstore",
		Name:      "read_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Duration per blobstore file read, in **microseconds**.",
	}, []string{
		BlobstoreTypeLabel,
	})

	BlobstoreWriteCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "blobstore",
		Name:      "write_count",
		Help:      "Number of files written to the blobstore.",
	}, []string{
		StatusLabel,
		BlobstoreTypeLabel,
	})

	/// ```promql
	/// # Bytes uploaded per second
	/// sum(rate(buildbuddy_blobstore_write_size_bytes[5m]))
	/// ```

	BlobstoreWriteSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "blobstore",
		Name:      "write_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Number of bytes written to the blobstore per file.",
	}, []string{
		BlobstoreTypeLabel,
	})

	BlobstoreWriteDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "blobstore",
		Name:      "write_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Duration per blobstore file write, in **microseconds**.",
	}, []string{
		BlobstoreTypeLabel,
	})

	BlobstoreDeleteCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "blobstore",
		Name:      "delete_count",
		Help:      "Number of files deleted from the blobstore.",
	}, []string{
		StatusLabel,
		BlobstoreTypeLabel,
	})

	BlobstoreDeleteDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "blobstore",
		Name:      "delete_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Delete duration per blobstore file deletion, in **microseconds**.",
	}, []string{
		BlobstoreTypeLabel,
	})

	/// # SQL metrics
	///
	/// The following metrics are for monitoring the SQL database configured
	/// for BuildBuddy.
	///
	/// If you'd like to see an up-to-date catalog of what BuildBuddy stores in
	/// its SQL database, see the table definitions [here](https://github.com/buildbuddy-io/buildbuddy/blob/master/server/tables/tables.go).
	///
	/// ## Query / error rate metrics

	SQLQueryCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "sql",
		Name:      "query_count",
		Help:      "Number of SQL queries executed.",
	}, []string{
		SQLQueryTemplateLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # SQL queries per second (by query template).
	/// sum by (sql_query_template) (rate(buildbuddy_sql_query_count[5m]))
	/// ```

	SQLQueryDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "sql",
		Name:      "query_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "SQL query duration, in **microseconds**.",
	}, []string{
		SQLQueryTemplateLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Median SQL query duration
	/// histogram_quantile(
	///	  0.5,
	///   sum(rate(buildbuddy_sql_query_duration_usec_bucket[5m])) by (le)
	/// )
	/// ```

	SQLErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "sql",
		Name:      "error_count",
		Help:      "Number of SQL queries that resulted in an error.",
	}, []string{
		SQLQueryTemplateLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # SQL error rate
	/// sum(rate(buildbuddy_sql_error_count[5m]))
	///   /
	/// sum(rate(buildbuddy_sql_query_count[5m]))
	/// ```

	/// ## `database/sql` metrics
	///
	/// The following metrics directly expose
	/// [DBStats](https://golang.org/pkg/database/sql/#DBStats) from the
	/// `database/sql` Go package.

	SQLMaxOpenConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "sql",
		Name:      "max_open_connections",
		Help:      "Maximum number of open connections to the database.",
	}, []string{
		SQLDBRoleLabel,
	})

	SQLOpenConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "sql",
		Name:      "open_connections",
		Help:      "The number of established connections to the database.",
	}, []string{
		SQLConnectionStatusLabel,
		SQLDBRoleLabel,
	})

	SQLWaitCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "sql",
		Name:      "wait_count",
		Help:      "The total number of connections waited for.",
	}, []string{
		SQLDBRoleLabel,
	})

	SQLWaitDuration = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "sql",
		Name:      "wait_duration_usec",
		Help:      "The total time blocked waiting for a new connection, in **microseconds**.",
	}, []string{
		SQLDBRoleLabel,
	})

	SQLMaxIdleClosed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "sql",
		Name:      "max_idle_closed",
		Help:      "The total number of connections closed due to SetMaxIdleConns.",
	}, []string{
		SQLDBRoleLabel,
	})

	SQLMaxIdleTimeClosed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "sql",
		Name:      "max_idle_time_closed",
		Help:      "The total number of connections closed due to SetConnMaxIdleTime.",
	}, []string{
		SQLDBRoleLabel,
	})

	SQLMaxLifetimeClosed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "sql",
		Name:      "max_lifetime_closed",
		Help:      "The total number of connections closed due to SetConnMaxLifetime.",
	}, []string{
		SQLDBRoleLabel,
	})

	/// ## HTTP metrics

	HTTPRequestCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "http",
		Name:      "request_count",
		Help:      "HTTP request count.",
	}, []string{
		HTTPRouteLabel,
		HTTPMethodLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Requests per second, by status code
	/// sum by (code) (rate(buildbuddy_http_request_count[5m]))
	///
	/// # 5xx error ratio
	/// sum(rate(buildbuddy_http_request_count{code=~"5.."}[5m]))
	///   /
	/// sum(rate(buildbuddy_http_request_count[5m]))
	/// ```

	HTTPRequestHandlerDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "http",
		Name:      "request_handler_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Time taken to handle each HTTP request in **microseconds**.",
	}, []string{
		HTTPRouteLabel,
		HTTPMethodLabel,
		HTTPResponseCodeLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Median request duration for successfuly processed (2xx) requests.
	/// # Other status codes may be associated with early-exits and are
	/// # likely to add too much noise.
	/// histogram_quantile(
	///   0.5,
	///   sum by (le)	(rate(buildbuddy_http_request_handler_duration_usec{code=~"2.."}[5m]))
	/// )
	/// ```

	HTTPResponseSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "http",
		Name:      "response_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Response size of each HTTP response in **bytes**.",
	}, []string{
		HTTPRouteLabel,
		HTTPMethodLabel,
		HTTPResponseCodeLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Median HTTP response size
	/// histogram_quantile(
	///   0.5,
	///   sum by (le)	(rate(buildbuddy_http_response_size_bytes[5m]))
	/// )
	/// ```

	/// ## Internal metrics
	///
	/// These metrics are for monitoring lower-level subsystems of BuildBuddy.
	///
	/// ### Build event handler
	///
	/// The build event handler logs all build events uploaded to BuildBuddy
	/// as part of the Build Event Protocol.

	BuildEventHandlerDurationUs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "build_event_handler",
		Name:      "duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "The time spent handling each build event in **microseconds**.",
	}, []string{
		StatusLabel,
	})

	/// ### Webhooks
	///
	/// Webhooks are HTTP endpoints exposed by BuildBuddy server which allow it to
	/// respond to repository events. These URLs are created as part of BuildBuddy
	/// workflows.

	WebhookHandlerWorkflowsStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "webhook_handler",
		Name:      "workflows_started",
		Help:      "The number of workflows triggered by the webhook handler.",
	}, []string{
		WebhookEventName,
	})

	/// ### Cache
	///
	/// "Cache" refers to the cache backend(s) that BuildBuddy uses to
	/// accelerate file IO operations, which are common in different
	/// subsystems such as the remote cache and the fetch server (for
	/// downloading invocation artifacts).
	///
	/// BuildBuddy can be configured to use multiple layers of caching
	/// (an in-memory layer, coupled with a cloud storage layer).
	///
	/// #### `get` metrics
	///
	/// `get` metrics track non-streamed cache reads (all data is fetched
	/// from the cache in a single request).

	CacheGetCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "get_count",
		Help:      "Number of cache get requests.",
	}, []string{
		StatusLabel,
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheGetDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "get_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "The time spent retrieving each entry from the cache, in **microseconds**. This is recorded only for successful gets.",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheGetSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "get_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Size of each entry retrieved from the cache, in **bytes**. This is recorded only for successful gets.",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	/// #### `read` metrics
	///
	/// `read` metrics track streamed cache reads.

	CacheReadCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "read_count",
		Help:      "Number of streamed cache reads started. This is incremented once for each started stream, **not** for each chunk in the stream.",
	}, []string{
		StatusLabel,
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheReadDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "read_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "The total time spent for each read stream, in **microseconds**. This is recorded only for successful reads, and measures the entire read stream (not just individual chunks).",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheReadSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "read_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Total size of each entry retrieved from the cache via streaming, in **bytes**. This is recorded only on success, and measures the entire stream (not just individual chunks).",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	/// #### `set` metrics
	///
	/// `set` metrics track non-streamed cache writes (all data is wrtiten
	/// in a single request).

	CacheSetCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "set_count",
		Help:      "Number of cache set requests.",
	}, []string{
		StatusLabel,
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheSetDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "set_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "The time spent writing each entry to the cache, in **microseconds**. This is recorded only for successful sets.",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheSetSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "set_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Size of the value stored in each set operation, in **bytes**. This is recorded only for successful sets.",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheSetRetryCount = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "set_retries",
		Buckets:   prometheus.LinearBuckets(0, 1, 10),
		Help:      "Number of retries required to fulfill the set request (an observed value of 0 means the transfer succeeded on the first try).",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	/// #### `write` metrics
	///
	/// `write` metrics track streamed cache writes.

	CacheWriteCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "write_count",
		Help:      "Number of streamed cache writes started. This is incremented once for each started stream, **not** for each chunk in the stream.",
	}, []string{
		StatusLabel,
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheWriteDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "write_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "The time spent for each streamed write to the cache, in **microseconds**. This is recorded only on success, and measures the entire stream (not just individual chunks).",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheWriteSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "write_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Size of each entry written to the cache via streaming, in **bytes**. This is recorded only on success, and measures the entire stream (not just individual chunks).",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheWriteRetryCount = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "write_retries",
		Buckets:   prometheus.LinearBuckets(0, 1, 10),
		Help:      "Number of retries required to write each chunk in the stream (an observed value of 0 means the transfer succeeded on the first try).",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	/// ### Other cache metrics

	CacheDeleteCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "delete_count",
		Help:      "Number of deletes from the cache.",
	}, []string{
		StatusLabel,
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheDeleteDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "delete_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Duration of each cache deletion, in **microseconds**.",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheContainsCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "contains_count",
		Help:      "Number of `contains(key)` requests made to the cache.",
	}, []string{
		StatusLabel,
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheContainsDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "contains_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Duration of each each `contains(key)` request, in **microseconds**.",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	CacheContainsRetryCount = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cache",
		Name:      "contains_retry_count",
		Buckets:   prometheus.LinearBuckets(0, 1, 10),
		Help:      "Number of retries required to fulfill each `contains(key)` request to the cache (an observed value of 0 means the request succeeded on the first try).",
	}, []string{
		CacheTierLabel,
		CacheBackendLabel,
	})

	/// ### Misc metrics

	UnexpectedEvent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Name:      "unexpected_event",
		Help:      "Counter for unexpected events.",
	}, []string{
		EventName,
	})
)
