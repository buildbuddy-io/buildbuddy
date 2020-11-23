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
	statusLabel = "status"
)

var (
	/// ## Build Event Handler
	///
	/// These metrics are for monitoring BuildBuddy's handling of build events.

	BuildEventHandlerDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "buildbuddy",
		Subsystem: "build_event_handler",
		Name:      "duration",
		Buckets:   buckets.HighVariabilityMicrosecondBuckets,
		Help:      "The time spent handling each build event in **microseconds**. Use the **`_count`** suffix to get the total number of build events handled.",
	}, []string{
		statusLabel,
	})

	/// ## Action Cache
	///
	/// These metrics are available when using BuildBuddy's remote cache.

	ActionCacheDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "buildbuddy",
		Subsystem: "action_cache",
		Name:      "duration",
		Buckets:   buckets.HighVariabilityMicrosecondBuckets,
		Help:      "The time spent handling each action cache request in **microseconds**. Use the **`_count`** suffix to get the total number of action cache requests handled.",
	}, []string{
		statusLabel,
		/// Cache event type: `hit`, `miss`, or `upload`.
		"event_type",
	})
)
