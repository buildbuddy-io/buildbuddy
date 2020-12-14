package cache

import (
	"fmt"
	"io"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/prometheus/client_golang/prometheus"

	gstatus "google.golang.org/grpc/status"
)

type CacheLayer string

const (
	MemoryCacheLayer CacheLayer = "memory"
	CloudCacheLayer  CacheLayer = "cloud"

	setMethodLabel   = "single"
	writeMethodLabel = "streaming"
)

// CacheMetrics exposes APIs for instrumenting cache operations.
type CacheMetrics struct {
	Layer   CacheLayer
	Backend string
}

func (c *CacheMetrics) NewInstrumentedReader(r io.Reader) *InstrumentedReader {
	return &InstrumentedReader{
		Reader: r,
		cacheMetrics: c,
	}
}

func (c *CacheMetrics) NewInstrumentedWriteCloser(w io.WriteCloser) *InstrumentedWriteCloser {
	return &InstrumentedWriteCloser{
		WriteCloser: w,
		cacheMetrics: c,
	}
}

// InstrumentedReader wraps a Reader with instrumentation.
type InstrumentedReader struct {
	io.Reader
	cacheMetrics *CacheMetrics
	timer *CacheOpTimer
	numBytesRead int
}

func (ir *InstrumentedReader) Read(p []byte) (int, error) {
	// Start the timer on the first read.
	if ir.timer == nil {
		ir.timer = ir.cacheMetrics.BeginCacheOperation()
	}

	n, err := ir.Reader.Read(p)

	ir.numBytesRead += n
	// Record metrics on the last read.
	if err != nil {
		ir.timer.EndStreamingRead(ir.numBytesRead, err)
	}
	return n, err
}

// InstrumentedWriteCloser wraps a WriteCloser with instrumentation.
//
// The wrapped writer is responsible for recording the number of
// retries performed during each call to Write.
type InstrumentedWriteCloser struct {
	io.WriteCloser
	cacheMetrics *CacheMetrics
	timer *CacheOpTimer
	numBytesWritten int
	ended bool
}

func (iwc *InstrumentedWriteCloser) Write(p []byte) (int, error) {
	if iwc.timer == nil {
		iwc.timer = iwc.cacheMetrics.BeginCacheOperation()
	}

	n, err := iwc.Write(p)

	iwc.numBytesWritten += n
	if err != nil && !iwc.ended {
		iwc.timer.EndStreamingWrite(iwc.numBytesWritten, err)
		iwc.ended = true
	}
	return n, err
}

func (iwc *InstrumentedWriteCloser) Close() error {
	err := iwc.Close()

	if !iwc.ended && iwc.timer != nil {
		iwc.timer.EndStreamingWrite(iwc.numBytesWritten, err)
		iwc.ended = true
	}
	return err
}

func (c *CacheMetrics) RecordSetRetries(numRetries int) {
	metrics.CacheSetRetryCount.With(prometheus.Labels{
		metrics.CacheLayerLabel:       string(c.Layer),
		metrics.CacheBackendLabel:     c.Backend,
	}).Observe(float64(numRetries))
}

func (c *CacheMetrics) RecordWriteRetries(numRetries int) {
	metrics.CacheWriteRetryCount.With(prometheus.Labels{
		metrics.CacheLayerLabel:       string(c.Layer),
		metrics.CacheBackendLabel:     c.Backend,
	}).Observe(float64(numRetries))
}

func (c *CacheMetrics) RecordContainsRetries(numRetries int) {
	metrics.CacheContainsRetryCount.With(prometheus.Labels{
		metrics.CacheLayerLabel:   string(c.Layer),
		metrics.CacheBackendLabel: c.Backend,
	}).Observe(float64(numRetries))
}

func (c *CacheMetrics) BeginCacheOperation() *CacheOpTimer {
	return &CacheOpTimer{
		cache:     c,
		startTime: time.Now(),
	}
}

type CacheOpTimer struct {
	cache     *CacheMetrics
	startTime time.Time
}

// Note: in all the following methods, we don't track duration if there's
// an error, but do track count (recording the status code) so we can measure
// failure rates.

func (t *CacheOpTimer) EndGet(size int, err error) {
	duration := time.Since(t.startTime)
	metrics.CacheGetCount.With(prometheus.Labels{
		metrics.StatusLabel:           fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheLayerLabel:       string(t.cache.Layer),
		metrics.CacheBackendLabel:     t.cache.Backend,
	}).Inc()
	if err != nil {
		return
	}
	metrics.CacheGetDurationUsec.With(prometheus.Labels{
		metrics.CacheLayerLabel:       string(t.cache.Layer),
		metrics.CacheBackendLabel:     t.cache.Backend,
	}).Observe(float64(duration.Microseconds()))
	metrics.CacheGetSizeBytes.With(prometheus.Labels{
		metrics.CacheLayerLabel:       string(t.cache.Layer),
		metrics.CacheBackendLabel:     t.cache.Backend,
	}).Observe(float64(size))
}

func (t *CacheOpTimer) EndSet(size int, err error) {
	duration := time.Since(t.startTime)
	metrics.CacheSetCount.With(prometheus.Labels{
		metrics.StatusLabel:           fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheLayerLabel:       string(t.cache.Layer),
		metrics.CacheBackendLabel:     t.cache.Backend,
	}).Inc()
	if err != nil {
		return
	}
	metrics.CacheSetDurationUsec.With(prometheus.Labels{
		metrics.CacheLayerLabel:       string(t.cache.Layer),
		metrics.CacheBackendLabel:     t.cache.Backend,
	}).Observe(float64(duration.Microseconds()))
	metrics.CacheSetSizeBytes.With(prometheus.Labels{
		metrics.CacheLayerLabel:       string(t.cache.Layer),
		metrics.CacheBackendLabel:     t.cache.Backend,
	}).Observe(float64(size))
}

func (t *CacheOpTimer) EndDelete(err error) {
	duration := time.Since(t.startTime)
	metrics.CacheDeleteCount.With(prometheus.Labels{
		metrics.StatusLabel:       fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheLayerLabel:   string(t.cache.Layer),
		metrics.CacheBackendLabel: t.cache.Backend,
	})
	if err != nil {
		return
	}
	metrics.CacheDeleteDurationUsec.With(prometheus.Labels{
		metrics.CacheLayerLabel:   string(t.cache.Layer),
		metrics.CacheBackendLabel: t.cache.Backend,
	}).Observe(float64(duration.Microseconds()))
}

func (t *CacheOpTimer) EndContains(err error) {
	duration := time.Since(t.startTime)
	metrics.CacheContainsCount.With(prometheus.Labels{
		metrics.StatusLabel:       fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheLayerLabel:   string(t.cache.Layer),
		metrics.CacheBackendLabel: t.cache.Backend,
	})
	if err != nil {
		return
	}
	metrics.CacheContainsDurationUsec.With(prometheus.Labels{
		metrics.CacheLayerLabel:   string(t.cache.Layer),
		metrics.CacheBackendLabel: t.cache.Backend,
	}).Observe(float64(duration.Microseconds()))
}

func (t *CacheOpTimer) EndStreamingRead(size int, err error) {
	duration := time.Since(t.startTime)
	metrics.CacheReadCount.With(prometheus.Labels{
		metrics.StatusLabel:       fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheLayerLabel:   string(t.cache.Layer),
		metrics.CacheBackendLabel: t.cache.Backend,
	}).Inc()
	if err != nil && err != io.EOF {
		return
	}
	metrics.CacheReadDurationUsec.With(prometheus.Labels{
		metrics.CacheLayerLabel:   string(t.cache.Layer),
		metrics.CacheBackendLabel: t.cache.Backend,
	}).Observe(float64(duration.Microseconds()))
	metrics.CacheReadSizeBytes.With(prometheus.Labels{
		metrics.CacheLayerLabel:   string(t.cache.Layer),
		metrics.CacheBackendLabel: t.cache.Backend,
	}).Observe(float64(size))
}

func (t *CacheOpTimer) EndStreamingWrite(size int, err error) {
	duration := time.Since(t.startTime)
	metrics.CacheWriteCount.With(prometheus.Labels{
		metrics.StatusLabel:           fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheLayerLabel:       string(t.cache.Layer),
		metrics.CacheBackendLabel:     t.cache.Backend,
	}).Inc()
	if err != nil {
		return
	}
	metrics.CacheWriteDurationUsec.With(prometheus.Labels{
		metrics.CacheLayerLabel:       string(t.cache.Layer),
		metrics.CacheBackendLabel:     t.cache.Backend,
	}).Observe(float64(duration.Microseconds()))
	metrics.CacheWriteSizeBytes.With(prometheus.Labels{
		metrics.CacheLayerLabel:       string(t.cache.Layer),
		metrics.CacheBackendLabel:     t.cache.Backend,
	}).Observe(float64(size))
}
