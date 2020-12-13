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

	/// Cache type: `gcs` (Google Cloud Storage) or `aws_s3`.
	CloudCacheTypeLabel = "cache_type"

	/// Cache write method: `stream` for streaming uploads to the cache,
	/// or `single` for single upload requests.
	CloudCacheWriteMethodLabel = "write_type"
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
		// TODO: Slice on build vs. test?
		InvocationStatusLabel,
	})

	InvocationDurationUs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "The total duration of each invocation, in **microseconds**.",
	}, []string{
		// TODO: Slice on build vs. test
		InvocationStatusLabel,
	})

	BuildEventCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "invocation",
		Name:      "build_event_count",
		Help:      "Number of [build events](https://docs.bazel.build/versions/master/build-event-protocol.html) uploaded to BuildBuddy.",
	}, []string{
		StatusLabel,
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

	CacheDownloadDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "download_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Download duration for each file downloaded from the remote cache, in **microseconds**.",
	}, []string{
		CacheTypeLabel,
	})

	CacheUploadSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "upload_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Number of bytes uploaded to the remote cache in each upload. Use the **`_sum`** suffix to get the total uploaded bytes and the **`_count`** suffix to get the number of uploaded files.",
	}, []string{
		CacheTypeLabel,
	})

	CacheUploadDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_cache",
		Name:      "upload_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Upload duration for each file uploaded to the remote cache, in **microseconds**.",
	}, []string{
		CacheTypeLabel,
	})

	/// ## Remote execution metrics

	RemoteExecutionCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "count",
		Help:      "Number of actions executed remotely.",
	}, []string{
		ExitCodeLabel,
	})

	RemoteExecutionQueueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: bbNamespace,
		Subsystem: "remote_execution",
		Name:      "queue_length",
		Help:      "Number of actions currently waiting in the executor queue.",
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
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Per-file upload duration during remote execution, in **microseconds**.",
	})

	/// ## SQL metrics
	///
	/// These metrics are for monitoring the configured SQL database.

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

	SQLErrorCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "sql",
		Name:      "error_count",
		Help:      "Number of SQL queries that resulted in an error.",
	})

	/// #### Examples
	///
	/// ```promql
	/// # SQL error rate
	/// sum(rate(buildbuddy_sql_error_count[5m]))
	///   /
	/// sum(rate(buildbuddy_sql_query_count[5m]))
	/// ```

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

	/// ### Cloud cache

	CloudCacheReadCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "read_count",
		Help:      "Number of reads from the cloud cache.",
	}, []string{
		StatusLabel,
		CloudCacheTypeLabel,
	})

	CloudCacheReadDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "read_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "The time spent reading each file from the cloud cache, in **microseconds**. This is recorded only for successful reads.",
	}, []string{
		CloudCacheTypeLabel,
	})

	CloudCacheReadSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "read_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Size of each file downloaded from the cloud cache, in **bytes**. This is recorded only for successful reads.",
	}, []string{
		CloudCacheTypeLabel,
	})

	CloudCacheWriteCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "write_count",
		Help:      "Number of `set(data)` requests to the cloud cache. This will be incremented both for the initial attempt as well as retries.",
	}, []string{
		StatusLabel,
		CloudCacheTypeLabel,
		CloudCacheWriteMethodLabel,
	})

	CloudCacheWriteDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "write_duration_usec",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "The time spent writing each file to the cloud cache, in **microseconds**. This is recorded only for successful writes.",
	}, []string{
		CloudCacheTypeLabel,
		CloudCacheWriteMethodLabel,
	})

	CloudCacheWriteSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "write_size_bytes",
		Buckets:   prometheus.ExponentialBuckets(1, 10, 9),
		Help:      "Size of each file uploaded to the cloud cache, in **bytes**. This is recorded only for successful writes.",
	}, []string{
		CloudCacheTypeLabel,
		CloudCacheWriteMethodLabel,
	})

	CloudCacheWriteRetryCount = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "write_retries",
		Buckets:   prometheus.LinearBuckets(0, 1, 10),
		Help:      "Number of retries required to fulfill each write request to the cloud cache (an observed value of 0 means the transfer succeeded on the first try).",
	}, []string{
		CloudCacheTypeLabel,
		CloudCacheWriteMethodLabel,
	})

	CloudCacheDeleteCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "delete_count",
		Help:      "Number of deletes from the cache.",
	}, []string{
		StatusLabel,
		CloudCacheTypeLabel,
	})

	CloudCacheDeleteDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "delete_duration_usec",
		Help:      "Duration of each cloud cache deletion, in **microseconds**.",
	}, []string{
		CloudCacheTypeLabel,
	})

	CloudCacheContainsCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "contains_count",
		Help:      "Number of `contains(key)` requests made to the cache.",
	}, []string{
		StatusLabel,
		CloudCacheTypeLabel,
	})

	CloudCacheContainsDurationUsec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "contains_duration_usec",
		Help:      "Duration of each each `contains(key)` request, in **microseconds**.",
	}, []string{
		CloudCacheTypeLabel,
	})

	CloudCacheContainsRetryCount = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: bbNamespace,
		Subsystem: "cloud_cache",
		Name:      "contains_retry_count",
		Help:      "Number of retries required to fulfill each `contains(key)` request to the cache (an observed value of 0 means the request succeeded on the first try).",
	}, []string{
		CloudCacheTypeLabel,
	})
)
