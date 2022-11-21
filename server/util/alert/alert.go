package alert

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/prometheus/client_golang/prometheus"
)

// UnexpectedEvent is used to indicate that something unexpected happened within the application.
// This causes a Prometheus counter to be incremented which in turn generates an alert.
//
// `name` is used as a metric label and thus should be a constant string. Do not include dynamic information.
//
// Use this for cases where it's worth knowing about the event, but not worth introducing a standalone metric & alert.
//
// Example usages:
//
//	alert.UnexpectedEvent("cannot_unmarshal_proto")
//	alert.UnexpectedEvent("cannot_unmarshal_proto", "invocation_id %s err: %s", invocation_id, err)
func UnexpectedEvent(name string, msgAndArgs ...interface{}) {
	metrics.UnexpectedEvent.With(prometheus.Labels{metrics.EventName: name}).Inc()
	logMsg := fmt.Sprintf("Unexpected event %q", name)
	if len(msgAndArgs) == 1 {
		logMsg += fmt.Sprintf(": %s", msgAndArgs[0])
	} else if len(msgAndArgs) > 1 {
		logMsg += ": "
		msg, ok := msgAndArgs[0].(string)
		if ok {
			logMsg += fmt.Sprintf(msg, msgAndArgs[1:]...)
		} else {
			logMsg += "<invalid args to UnexpectedEvent>"
		}
	}
	log.Warning(logMsg)
}
