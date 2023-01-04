package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Note: the doc generator script (`generate_docs.py`) in this directory
// generates documentation from this file at build time.
//
// The doc generator treats comments starting with 3 slashes as markdown docs,
// as well as the 'Help' field for each metric.

const (
	// Label constants.
	// Commonly used labels can be added here, and their documentation will be
	// displayed in the metrics where they are used.

	// TODO(bduffany): Migrate StatusLabel usages to StatusHumanReadableLabel

	/// Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
	/// This is a numeric value; any non-zero code indicates an error.
	StatusLabel = "status"

	/// Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code)
	/// in human-readable format, such as "OK" or "NotFound".
	StatusHumanReadableLabel = "status"

	/// Invocation status: `success`, `failure`, `disconnected`, or `unknown`.
	InvocationStatusLabel = "invocation_status"

	/// Cache type: `action` for action cache, `cas` for content-addressable storage.
	CacheTypeLabel = "cache_type"

	/// Cache event type: `hit`, `miss`, or `upload`.
	CacheEventTypeLabel = "cache_event_type"

	/// Cache name: Custom name to describe the cache, like "pebble-cache".
	CacheNameLabel = "cache_name"

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

	/// Exit code of a completed bazel command
	BazelExitCode = "bazel_exit_code"

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

	/// Effective workload isolation type used for an executed task, such as
	/// "docker", "podman", "firecracker", or "none".
	IsolationTypeLabel = "isolation"

	/// Group (organization) ID associated with the request.
	GroupID = "group_id"

	/// OS associated with the request.
	OS = "os"

	/// CPU architecture associated with the request.
	Arch = "arch"

	/// The name used to identify the type of an unexpected event.
	EventName = "name"

	/// The ID of the disk cache partition this event applied to.
	PartitionID = "partition_id"

	/// Status of the file cache request: `hit` if found in cache, `miss` otherwise.
	FileCacheRequestStatusLabel = "status"

	/// Status of the task size read request: `hit`, `miss`, or `error`.
	TaskSizeReadStatusLabel = "status"

	/// Status of the task size write request: `ok`, `missing_stats` or `error`.
	TaskSizeWriteStatusLabel = "status"

	/// The full name of the grpc method: `/<service>/<method>`
	GRPCFullMethodLabel = "grpc_full_method"

	/// The key used for quota accounting, either a group ID or an IP address.
	QuotaKey = "quota_key"

	/// Whether the request was allowed by quota manager.
	QuotaAllowed = "quota_allowed"

	// Describes the type of cache request
	CacheRequestType = "type"

	// Describes the type of compression
	CompressionType = "compression"

	/// The name of the table in Clickhouse
	ClickhouseTableName = "clickhouse_table_name"

	/// Status of the Clickhouse operation: `ok`, `error`.
	ClickhouseStatusLabel = "status"

	/// The ID of a raft nodehost.
	RaftNodeHostIDLabel = "node_host_id"

	/// The range ID of a raft region.
	RaftRangeIDLabel = "range_id"

	/// The type of raft move `add`, or `remove`.
	RaftMoveLabel = "move_type"

	/// Raft RangeCache event type: `hit`, `miss`, or `update`.
	RaftRangeCacheEventTypeLabel = "rangecache_event_type"
)

const (
	bbNamespace = "buildbuddy"

	day = 24 * time.Hour
)

var (
	// Bucket constants.

	coarseMicrosecondToHour = durationUsecBuckets(1*time.Microsecond, 1*time.Hour, 10)
)

var (
	/// ## Invocation build event metrics
	///
	/// All invocation metrics are recorded at the _end_ of each invocation.

	InvocationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "count",
		Help:      "The total number of invocations whose logs were uploaded to BuildBuddy.",
	}, []string{
		InvocationStatusLabel,
		BazelExitCode,
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
		Buckets:   durationUsecBuckets(1*time.Millisecond, 1*day, 10),
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

	StatsRecorderWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "stats_recorder_workers",
		Help:      "Number of invocation stats recorder workers currently running.",
	})

	StatsRecorderDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "stats_recorder_duration_usec",
		Buckets:   coarseMicrosecondToHour,
		Help:      "How long it took to finalize an invocation's stats, in **microseconds**. This includes the time required to wait for all BuildBuddy apps to flush their local metrics to Redis (if applicable) and then record the metrics to the DB.",
	})

	WebhookInvocationLookupWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "webhook_invocation_lookup_workers",
		Help:      "Number of webhook invocation lookup workers currently running.",
	})

	WebhookInvocationLookupDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "webhook_invocation_lookup_duration_usec",
		Buckets:   coarseMicrosecondToHour,
		Help:      "How long it took to lookup an invocation before posting to the webhook, in **microseconds**.",
	})

	WebhookNotifyWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "webhook_notify_workers",
		Help:      "Number of webhook notify workers currently running.",
	})

	WebhookNotifyDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "webhook_notify_duration_usec",
		Buckets:   coarseMicrosecondToHour,
		Help:      "How long it took to post an invocation proto to the webhook, in **microseconds**.",
	})

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
		Buckets:   durationUsecBuckets(1*time.Microsecond, 1*time.Hour, 5),
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
		Buckets:   durationUsecBuckets(1*time.Microsecond, 1*time.Hour, 5),
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

	DiskCacheLastEvictionAgeUsec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "disk_cache_last_eviction_age_usec",
		Help:      "The age of the item most recently evicted from the cache, in **microseconds**.",
	}, []string{
		PartitionID,
		CacheNameLabel,
	})

	// This metric is in milliseconds because Grafana heatmaps don't display
	// microsecond durations nicely when they can contain large durations.
	DiskCacheEvictionAgeMsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "disk_cache_eviction_age_msec",
		Buckets:   exponentialBucketRange(float64(1*time.Hour.Milliseconds()), float64(30*24*time.Hour.Milliseconds()), 2),
		Help:      "Age of items evicted from the cache, in **milliseconds**.",
	}, []string{
		PartitionID,
		CacheNameLabel,
	})

	DiskCacheNumEvictions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "disk_cache_num_evictions",
		Help:      "Number of items evicted.",
	}, []string{
		PartitionID,
		CacheNameLabel,
	})

	DiskCachePartitionSizeBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "disk_cache_partition_size_bytes",
		Help:      "Number of bytes in the partition.",
	}, []string{
		PartitionID,
		CacheNameLabel,
	})

	DiskCachePartitionCapacityBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "disk_cache_partition_capacity_bytes",
		Help:      "Number of bytes in the partition.",
	}, []string{
		PartitionID,
		CacheNameLabel,
	})

	DiskCachePartitionNumItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "disk_cache_partition_num_items",
		Help:      "Number of items in the partition.",
	}, []string{
		PartitionID,
		CacheNameLabel,
		CacheTypeLabel,
	})

	DiskCacheDuplicateWrites = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "disk_cache_duplicate_writes",
		Help:      "Number of writes for digests that already exist.",
	}, []string{
		CacheNameLabel,
	})

	DiskCacheAddedFileSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "disk_cache_added_file_size_bytes",
		Help:      "Size of artifacts added to the file cache, in **bytes**.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 40),
	}, []string{
		CacheNameLabel,
	})

	DiskCacheFilesystemTotalBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "disk_cache_filesystem_total_bytes",
		Help:      "Total size of the underlying filesystem.",
	}, []string{
		CacheNameLabel,
	})

	DiskCacheFilesystemAvailBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "disk_cache_filesystem_avail_bytes",
		Help:      "Available bytes in the underlying filesystem.",
	}, []string{
		CacheNameLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Total number of duplicate writes.
	/// sum(buildbuddy_remote_cache_duplicate_writes)
	/// ```

	DiskCacheDuplicateWritesBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "disk_cache_duplicate_writes_bytes",
		Help:      "Number of bytes written that already existed in the cache.",
	}, []string{
		CacheNameLabel,
	})

	MigrationNotFoundErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "migration_not_found_error_count",
		Help:      "Number of not found errors from the destination cache during a cache migration.",
	}, []string{
		CacheRequestType,
	})

	MigrationDoubleReadHitCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "migration_double_read_hit_count",
		Help:      "Number of double reads where the source and destination caches hold the same digests during a cache migration.",
	}, []string{
		CacheRequestType,
	})

	MigrationCopyChanSize = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "migration_copy_chan_size",
		Help:      "Number of digests queued to be copied during a cache migration.",
	})

	MigrationBytesCopied = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "migration_bytes_copied",
		Help:      "Number of bytes copied from the source to destination cache during a cache migration.",
	}, []string{
		CacheTypeLabel,
	})

	MigrationBlobsCopied = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "migration_blobs_copied",
		Help:      "Number of blobs copied from the source to destination cache during a cache migration.",
	}, []string{
		CacheTypeLabel,
	})

	/// ## Remote execution metrics

	RemoteExecutionCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "count",
		Help:      "Number of actions executed remotely. This only includes actions which reached the execution phase. If an action fails before execution (for example, if it fails authentication) then this metric is not incremented.",
	}, []string{
		ExitCodeLabel,
		StatusHumanReadableLabel,
		IsolationTypeLabel,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Total number of actions executed per second
	/// sum(rate(buildbuddy_remote_execution_count[5m]))
	/// ```

	RemoteExecutionTasksStartedCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "tasks_started_count",
		Help:      "Number of tasks started remotely, but not necessarily completed. Includes retry attempts of the same task.",
	})

	RemoteExecutionExecutedActionMetadataDurationsUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "executed_action_metadata_durations_usec",
		Buckets:   durationUsecBuckets(1*time.Microsecond, 1*day, 5),
		Help:      "Time spent in each stage of action execution, in **microseconds**. Queries should filter or group by the `stage` label, taking care not to aggregate different stages.",
	}, []string{
		ExecutedActionStageLabel,
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

	RemoteExecutionTaskSizeReadRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "task_size_read_requests",
		Help:      "Number of read requests to the task sizer, which estimates action resource usage based on historical execution stats.",
	}, []string{
		TaskSizeReadStatusLabel,
		IsolationTypeLabel,
		OS,
		Arch,
		GroupID,
	})

	RemoteExecutionTaskSizeWriteRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "task_size_write_requests",
		Help:      "Number of write requests to the task sizer, which estimates action resource usage based on historical execution stats.",
	}, []string{
		TaskSizeWriteStatusLabel,
		IsolationTypeLabel,
		OS,
		Arch,
		GroupID,
	})

	RemoteExecutionTaskSizePredictionDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "task_size_prediction_duration_usec",
		Help:      "Task size prediction model request duration in **microseconds**.",
		Buckets:   durationUsecBuckets(1*time.Microsecond, 1*time.Second, 2),
	}, []string{
		StatusHumanReadableLabel,
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
	/// # Total number of execution requests with client waiting for result.
	/// sum(buildbuddy_remote_execution_waiting_execution_result)
	/// ```

	RemoteExecutionRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "requests",
		Help:      "Number of execution requests received.",
	}, []string{
		GroupID,
		OS,
		Arch,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Rate of new execution requests by OS/Arch.
	/// sum(rate(buildbuddy_remote_execution_requests[1m])) by (os, arch)
	/// ```

	RemoteExecutionMergedActions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "merged_actions",
		Help:      "Number of identical execution requests that have been merged.",
	}, []string{
		GroupID,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Rate of merged actions by group.
	/// sum(rate(buildbuddy_remote_execution_merged_actions[1m])) by (group_id)
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

	RemoteExecutionTasksExecuting = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "tasks_executing",
		Help:      "Number of tasks currently being executed by the executor.",
	}, []string{
		ExecutedActionStageLabel,
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

	RemoteExecutionAssignableRAMBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "assignable_ram_bytes",
		Help:      "Maximum total RAM that can be allocated for task execution, in **bytes**.",
	})

	RemoteExecutionAssignedMilliCPU = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "assigned_milli_cpu",
		Help:      "Estimated CPU time on the executor that is currently allocated for task execution, in **milliCPU** (CPU-milliseconds per second).",
	})

	RemoteExecutionAssignableMilliCPU = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "assignable_milli_cpu",
		Help:      "Maximum total CPU time on the executor that can be allocated for task execution, in **milliCPU** (CPU-milliseconds per second).",
	})

	RemoteExecutionMemoryUsageBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "memory_usage_bytes",
		Help:      "Current total task memory usage in **bytes**. This only accounts for tasks which are actively executing. To see memory usage of pooled runners, sum with runner pool memory usage.",
	})

	/// #### Examples
	///
	/// ```promql
	/// # Total approximate memory usage of active and pooled runners,
	/// # grouped by executor pod.
	/// sum by (pod_name) (
	///   buildbuddy_remote_execution_memory_usage_bytes
	///   + buildbuddy_remote_execution_runner_pool_memory_usage_bytes
	/// )
	/// ```

	RemoteExecutionPeakMemoryUsageBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "peak_memory_usage_bytes",
		Help:      "Current total peak memory usage in **bytes**. This is the sum of the peak memory usage for all tasks currently executing. It is not a very useful metric on its own, and is mainly intended for comparison with `assigned_ram_bytes`.",
	})

	RemoteExecutionUsedMilliCPU = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "used_milli_cpu",
		Help:      "Approximate cumulative CPU usage of executed tasks, in **CPU-milliseconds**.",
	})

	RemoteExecutionCPUUtilization = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "cpu_utilization_milli_cpu",
		Help:      "Approximate current CPU utilization of tasks executing, in **milli-CPU** (CPU-milliseconds per second). This allows for much higher granularity than using a `rate()` on `used_milli_cpu` metric.",
	})

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
		Buckets:   coarseMicrosecondToHour,
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

	FileCacheRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "file_cache_requests",
		Help:      "Number of local executor file cache requests.",
	}, []string{
		FileCacheRequestStatusLabel,
	})

	FileCacheLastEvictionAgeUsec = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "file_cache_last_eviction_age_usec",
		Help:      "Age of the last entry evicted from the executor's local file cache (relative to when it was added to the cache), in **microseconds**.",
	})

	FileCacheAddedFileSizeBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "file_cache_added_file_size_bytes",
		Help:      "Size of artifacts added to the file cache, in **bytes**.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 40),
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
		Buckets:   coarseMicrosecondToHour,
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
		Buckets:   coarseMicrosecondToHour,
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
		Buckets:   coarseMicrosecondToHour,
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
		Buckets:   coarseMicrosecondToHour,
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
		Buckets:   coarseMicrosecondToHour,
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
		Buckets:   coarseMicrosecondToHour,
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
		Buckets:   coarseMicrosecondToHour,
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
		Buckets:   coarseMicrosecondToHour,
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
		Buckets:   coarseMicrosecondToHour,
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
		Buckets:   coarseMicrosecondToHour,
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
		Buckets:   coarseMicrosecondToHour,
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
		Buckets:   coarseMicrosecondToHour,
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

	RPCsHandledTotalByQuotaKey = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "quota",
		Name:      "rpcs_handled_total_by_quota_key",
		Help:      "Total number of RPCs completed on the server by quota_key, regardless of success or failure.",
	}, []string{
		GRPCFullMethodLabel,
		QuotaKey,
		QuotaAllowed,
	})

	RegistryBlobRangeLatencyUsec = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "registry",
		Name:      "blob_range_latency_usec",
		Help:      "Latency of serving layer blob ranges.",
		Buckets:   coarseMicrosecondToHour,
	})

	ClickhouseInsertedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "clickhouse",
		Name:      "insert_count",
		Help:      "Num of rows inserted into clickhouse",
	}, []string{
		ClickhouseTableName,
		ClickhouseStatusLabel,
	})

	BytesCompressed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "compressor",
		Name:      "bytes_compressed",
		Help:      "The number of decompressed bytes passed into compressors",
	}, []string{
		CompressionType,
	})

	BytesDecompressed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "compressor",
		Name:      "bytes_decompressed",
		Help:      "The number of decompressed bytes passed out of decompressors",
	}, []string{
		CompressionType,
	})

	CompressionRatio = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "pebble",
		Name:      "compression_ratio",
		Buckets:   prometheus.LinearBuckets(0, .05, 40),
		Help: "The aggregate compression ratio (compressed / decompressed bytes) for a stream of data " +
			"(as opposed to being calculated on a per-chunk basis for data in the stream)",
	}, []string{
		CacheNameLabel,
		CompressionType,
	})

	/// #### Examples
	///
	/// ```promql
	/// # Histogram buckets with the count of elements in each compression ratio bucket
	/// # Visualize with the Bar Gauge type
	/// # Legend: {{le}}
	/// # Format: Heatmap
	/// sum(buildbuddy_pebble_compression_ratio_bucket) by(le)
	///
	/// # Percentage of elements that increased in size when compressed (compression ratio > 1)
	/// # Visualize with the Stat type
	/// (sum(buildbuddy_pebble_compression_ratio_count) - sum(buildbuddy_pebble_compression_ratio_bucket{le="1.0"})) / sum(buildbuddy_pebble_compression_ratio_count)
	/// ```

	/// ### Raft cache metrics

	RaftRanges = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "raft",
		Name:      "ranges",
		Help:      "Number of raft ranges on each nodehost.",
	}, []string{
		RaftNodeHostIDLabel,
	})

	RaftRecords = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "raft",
		Name:      "records",
		Help:      "Number of raft records in each range.",
	}, []string{
		RaftRangeIDLabel,
	})

	RaftBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "raft",
		Name:      "bytes",
		Help:      "Size (in bytes) of each range.",
	}, []string{
		RaftRangeIDLabel,
	})

	RaftProposals = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "raft",
		Name:      "proposals",
		Help:      "The total number of statemachine proposals on each range.",
	}, []string{
		RaftRangeIDLabel,
	})

	RaftSplits = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "raft",
		Name:      "splits",
		Help:      "The total number of splits per nodehost.",
	}, []string{
		RaftNodeHostIDLabel,
	})

	RaftMoves = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "raft",
		Name:      "moves",
		Help:      "The total number of moves per nodehost.",
	}, []string{
		RaftNodeHostIDLabel,
		RaftMoveLabel,
	})

	RaftRangeCacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "raft",
		Name:      "rangecache_lookups",
		Help:      "The total number of rangecache lookups per nodehost.",
	}, []string{
		RaftRangeCacheEventTypeLabel,
	})

	RaftSplitDurationUs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "raft",
		Name:      "split_duration_usec",
		Buckets:   coarseMicrosecondToHour,
		Help:      "The time spent splitting a range in **microseconds**.",
	}, []string{
		RaftRangeIDLabel,
	})

	// This metric is in milliseconds because Grafana heatmaps don't display
	// microsecond durations nicely when they can contain large durations.
	RaftEvictionAgeMsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "raft",
		Name:      "eviction_age_msec",
		Buckets:   exponentialBucketRange(float64(1*time.Hour.Milliseconds()), float64(30*24*time.Hour.Milliseconds()), 2),
		Help:      "Age of items evicted from the cache, in **milliseconds**.",
	}, []string{
		PartitionID,
	})
)

// exponentialBucketRange returns prometheus.ExponentialBuckets specified in
// terms of a min and max value, rather than needing to explicitly calculate the
// number of buckets.
func exponentialBucketRange(min, max, factor float64) []float64 {
	buckets := []float64{}
	current := min
	for current < max {
		buckets = append(buckets, current)
		current *= factor
	}
	return buckets
}

func durationUsecBuckets(min, max time.Duration, factor float64) []float64 {
	return exponentialBucketRange(float64(min.Microseconds()), float64(max.Microseconds()), factor)
}
