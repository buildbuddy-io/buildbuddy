package alert

import (
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// UnexpectedEvent is used to indicate that something unexpected happened within the application.
// This causes a Prometheus counter to be incremented which in turn generates an alert.
//
// `name` is used as a metric label and thus should be a constant string. Do not include dynamic information.
//
// Use this for cases where it's worth knowing about the event, but not worth introducing a standalone metric & alert.
func UnexpectedEvent(name string) {
	metrics.UnexpectedEvent.With(prometheus.Labels{metrics.EventName: name}).Inc()
}
