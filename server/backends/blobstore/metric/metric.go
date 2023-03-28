package metric

import (
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/prometheus/client_golang/prometheus"

	gstatus "google.golang.org/grpc/status"
)

func RecordWriteMetrics(typeLabel string, startTime time.Time, size int, err error) {
	duration := time.Since(startTime)
	metrics.BlobstoreWriteCount.With(prometheus.Labels{
		metrics.StatusLabel:        fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.BlobstoreTypeLabel: typeLabel,
	}).Inc()
	// Don't track duration or size if there's an error, but do track
	// count (above) so we can measure failure rates.
	if err != nil {
		return
	}
	metrics.BlobstoreWriteDurationUsec.With(prometheus.Labels{
		metrics.BlobstoreTypeLabel: typeLabel,
	}).Observe(float64(duration.Microseconds()))
	metrics.BlobstoreWriteSizeBytes.With(prometheus.Labels{
		metrics.BlobstoreTypeLabel: typeLabel,
	}).Observe(float64(size))
}

func RecordReadMetrics(typeLabel string, startTime time.Time, b []byte, err error) {
	duration := time.Since(startTime)
	size := len(b)
	metrics.BlobstoreReadCount.With(prometheus.Labels{
		metrics.StatusLabel:        fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.BlobstoreTypeLabel: typeLabel,
	}).Inc()
	// Don't track duration or size if there's an error, but do track
	// count (above) so we can measure failure rates.
	if err != nil {
		return
	}
	metrics.BlobstoreReadDurationUsec.With(prometheus.Labels{
		metrics.BlobstoreTypeLabel: typeLabel,
	}).Observe(float64(duration.Microseconds()))
	metrics.BlobstoreReadSizeBytes.With(prometheus.Labels{
		metrics.BlobstoreTypeLabel: typeLabel,
	}).Observe(float64(size))
}

func RecordDeleteMetrics(typeLabel string, startTime time.Time, err error) {
	duration := time.Since(startTime)
	metrics.BlobstoreDeleteCount.With(prometheus.Labels{
		metrics.StatusLabel:        fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.BlobstoreTypeLabel: typeLabel,
	})
	// Don't track duration if there's an error, but do track
	// count (above) so we can measure failure rates.
	if err != nil {
		return
	}
	metrics.BlobstoreDeleteDurationUsec.With(prometheus.Labels{
		metrics.BlobstoreTypeLabel: typeLabel,
	}).Observe(float64(duration.Microseconds()))
}
