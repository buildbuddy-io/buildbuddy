package metrics

import (
	"github.com/buildbuddy-io/buildbuddy/server/metrics/buckets"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	// Label constants

	// Status code as defined by [grpc/codes](https://godoc.org/google.golang.org/grpc/codes#Code).
	statusLabel = "status"
)

var (
	BuildEventHandlerDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "buildbuddy",
		Subsystem: "build_event_handler",
		Name:      "duration",
		Buckets:   buckets.HighVariabilityMicrosecondBuckets,
		Help:      "The time spent handling each build event in microseconds. Use the `_count` suffix to get the total number of build events handled.",
	}, []string{
		statusLabel,
	})
)
