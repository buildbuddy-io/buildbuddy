package cache_metrics

import (
	"fmt"
	"io"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/prometheus/client_golang/prometheus"

	gstatus "google.golang.org/grpc/status"
)

// CacheTier is an enumerated label value that describes the level at
// which the cache sits in the composable cache stack.
//
// This label enables Prometheus queries that don't break when the cache implementation
// is changed. For example, to get cache metrics for the in-memory tier only (without having
// to explicitly refer to "redis", "memcached", etc.), one could write this query:
//
//	sum by (backend) (rate(buildbuddy_cache_write_count{tier="memory"}[5m])
//
// These types of queries are useful when monitoring the cache because caches
// at different tiers will have observed latencies at different orders of magnitude,
// so each tier likely needs its own chart in order to be observed at an appropriate
// resolution.
type CacheTier string

const (
	// MemoryCacheTier is used for in-memory cache implementations, like memcached and redis.
	MemoryCacheTier CacheTier = "memory"
	// CloudCacheTier is used for cloud storage cache implementations, like GCS or AWS S3.
	CloudCacheTier CacheTier = "cloud"
)

func MakeCacheLabels(tier CacheTier, backend string) prometheus.Labels {
	return prometheus.Labels{
		metrics.CacheTierLabel:    string(tier),
		metrics.CacheBackendLabel: backend,
	}
}

func appendLabels(a prometheus.Labels, b prometheus.Labels) prometheus.Labels {
	labels := prometheus.Labels{}
	for k, v := range a {
		labels[k] = v
	}
	for k, v := range b {
		labels[k] = v
	}
	return labels
}

// InstrumentedReader wraps a Reader and CacheTimer so that the embedded timer is stopped
// and metrics are recorded on the last read.
type InstrumentedReader struct {
	io.Reader
	timer         *CacheTimer
	readSizeBytes int64
}

func (ir *InstrumentedReader) Read(p []byte) (int, error) {
	n, err := ir.Reader.Read(p)
	// Record metrics on the last read.
	if err != nil {
		ir.timer.ObserveRead(ir.readSizeBytes, err)
	}
	return n, err
}

func RecordSetRetries(labels prometheus.Labels, numRetries int) {
	metrics.CacheSetRetryCount.With(labels).Observe(float64(numRetries))
}

func RecordWriteRetries(labels prometheus.Labels, numRetries int) {
	metrics.CacheWriteRetryCount.With(labels).Observe(float64(numRetries))
}

func RecordContainsRetries(labels prometheus.Labels, numRetries int) {
	metrics.CacheContainsRetryCount.With(labels).Observe(float64(numRetries))
}

type CacheTimer struct {
	labels    prometheus.Labels
	startTime time.Time
}

func NewCacheTimer(labels prometheus.Labels) *CacheTimer {
	return &CacheTimer{
		labels:    labels,
		startTime: time.Now(),
	}
}

func (t *CacheTimer) NewInstrumentedReader(r io.Reader, size int64) *InstrumentedReader {
	return &InstrumentedReader{
		Reader:        r,
		timer:         t,
		readSizeBytes: size,
	}
}

// Note: in all the following methods, we don't track duration if there's
// an error, but do track count (recording the status code) so we can measure
// failure rates.

func (t *CacheTimer) ObserveGet(size int, err error) {
	duration := time.Since(t.startTime)
	metrics.CacheGetCount.With(appendLabels(t.labels, prometheus.Labels{
		metrics.StatusLabel: fmt.Sprintf("%d", gstatus.Code(err)),
	})).Inc()
	if err != nil {
		return
	}
	metrics.CacheGetDurationUsec.With(t.labels).Observe(float64(duration.Microseconds()))
	metrics.CacheGetSizeBytes.With(t.labels).Observe(float64(size))
}

func (t *CacheTimer) ObserveSet(size int, err error) {
	duration := time.Since(t.startTime)
	metrics.CacheSetCount.With(appendLabels(t.labels, prometheus.Labels{
		metrics.StatusLabel: fmt.Sprintf("%d", gstatus.Code(err)),
	})).Inc()
	if err != nil {
		return
	}
	metrics.CacheSetDurationUsec.With(t.labels).Observe(float64(duration.Microseconds()))
	metrics.CacheSetSizeBytes.With(t.labels).Observe(float64(size))
}

func (t *CacheTimer) ObserveDelete(err error) {
	duration := time.Since(t.startTime)
	metrics.CacheDeleteCount.With(appendLabels(t.labels, prometheus.Labels{
		metrics.StatusLabel: fmt.Sprintf("%d", gstatus.Code(err)),
	})).Inc()
	if err != nil {
		return
	}
	metrics.CacheDeleteDurationUsec.With(t.labels).Observe(float64(duration.Microseconds()))
}

func (t *CacheTimer) ObserveContains(err error) {
	duration := time.Since(t.startTime)
	metrics.CacheContainsCount.With(appendLabels(t.labels, prometheus.Labels{
		metrics.StatusLabel: fmt.Sprintf("%d", gstatus.Code(err)),
	})).Inc()
	if err != nil {
		return
	}
	metrics.CacheContainsDurationUsec.With(t.labels).Observe(float64(duration.Microseconds()))
}

func (t *CacheTimer) ObserveRead(size int64, err error) {
	duration := time.Since(t.startTime)
	metrics.CacheReadCount.With(appendLabels(t.labels, prometheus.Labels{
		metrics.StatusLabel: fmt.Sprintf("%d", gstatus.Code(err)),
	})).Inc()
	if err != nil && err != io.EOF {
		return
	}
	metrics.CacheReadDurationUsec.With(t.labels).Observe(float64(duration.Microseconds()))
	metrics.CacheReadSizeBytes.With(t.labels).Observe(float64(size))
}

func (t *CacheTimer) ObserveWrite(size int64, err error) {
	duration := time.Since(t.startTime)
	metrics.CacheWriteCount.With(appendLabels(t.labels, prometheus.Labels{
		metrics.StatusLabel: fmt.Sprintf("%d", gstatus.Code(err)),
	})).Inc()
	if err != nil {
		return
	}
	metrics.CacheWriteDurationUsec.With(t.labels).Observe(float64(duration.Microseconds()))
	metrics.CacheWriteSizeBytes.With(t.labels).Observe(float64(size))
}
