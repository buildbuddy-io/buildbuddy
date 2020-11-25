package metrics

import (
	"github.com/buildbuddy-io/buildbuddy/server/metrics/buckets"
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

	// TODO(bduffany): Explain the difference between `miss` and `upload`
	/// Cache event type: `hit`, `miss`, or `upload`.
	CacheEventTypeLabel = "cache_event_type"
)

var (
	/// ## Invocation metrics (builds and tests)
	///
	/// NOTE: Invocation metrics are recorded at the end of each invocation,
	/// which means that these metrics provide _approximate_ real-time signals.

	InvocationDurationUs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "buildbuddy",
		Subsystem: "invocation",
		Name:      "duration_us",
		Buckets:   []float64{0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000},
		Help:      "The total duration of each invocation, in **microseconds**.",
	}, []string{
		// TODO: Slice on build vs. test
		InvocationStatusLabel,
	})

	/// ## Remote cache metrics
	///
	/// NOTE: Cache metrics are recorded at the end of each invocation,
	/// which means that these metrics provide _approximate_ real-time signals.

	CacheEvents = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "buildbuddy",
		Subsystem: "remote_cache",
		Name:      "events",
		Buckets:   []float64{0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000},
		Help:      "Number of cache events handled in each invocation. Use the **`_sum`** suffix to get the total number of cache events across all invocations.",
	}, []string{
		InvocationStatusLabel,
		CacheTypeLabel,
		CacheEventTypeLabel,
	})

	CacheDownloadSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "buildbuddy",
		Subsystem: "remote_cache",
		Name:      "download_size_bytes",
		Buckets:   []float64{0, 1_000, 1_000_000, 1_000_000_000},
		Help:      "Number of bytes downloaded from the remote cache per invocation. Use the **`_sum`** suffix to get the total downloaded bytes across all invocations.",
	}, []string{
		InvocationStatusLabel,
	})

	CacheUploadSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "buildbuddy",
		Subsystem: "remote_cache",
		Name:      "upload_size_bytes",
		Buckets:   []float64{0, 1_000, 1_000_000, 1_000_000_000},
		Help:      "Number of bytes uploaded to the remote cache per invocation. Use the **`_sum`** suffix to get the total uploaded bytes across all invocations.",
	}, []string{
		InvocationStatusLabel,
	})

	/// ## Internal metrics
	///
	/// These metrics are for monitoring lower-level subsystems of BuildBuddy.
	///
	/// ### Build event handler
	///
	/// The build event handler logs all build events uploaded to BuildBuddy
	/// as part of the Build Event Protocol.

	BuildEventHandlerDurationUs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "buildbuddy",
		Subsystem: "build_event_handler",
		Name:      "duration_us",
		Buckets:   buckets.HighVariabilityMicrosecondBuckets,
		Help:      "The time spent handling each build event in **microseconds**. Use the **`_count`** suffix to get the total number of build events handled.",
	}, []string{
		StatusLabel,
	})
)
