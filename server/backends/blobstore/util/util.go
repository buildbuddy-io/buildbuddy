package util

import (
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"

	gstatus "google.golang.org/grpc/status"
)

var (
	pathPrefix = flag.String("storage.path_prefix", "", "The prefix directory to store all blobs in")
)

func Decompress(in []byte, err error) ([]byte, error) {
	if err != nil {
		return in, err
	}

	var buf bytes.Buffer
	// Write instead of using NewBuffer because if this is not a gzip file
	// we want to return "in" directly later, and NewBuffer would take
	// ownership of it.
	if _, err := buf.Write(in); err != nil {
		return nil, err
	}
	zr, err := gzip.NewReader(&buf)
	if err == gzip.ErrHeader {
		// Compatibility hack: if we got a header error it means this
		// is probably an uncompressed record written before we were
		// compressing. Just read it as-is.
		return in, nil
	}
	if err != nil {
		return nil, err
	}
	rsp, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	if err := zr.Close(); err != nil {
		return nil, err
	}
	return rsp, nil
}

func Compress(in []byte) ([]byte, error) {
	var buf bytes.Buffer
	zr := gzip.NewWriter(&buf)
	if _, err := zr.Write(in); err != nil {
		return nil, err
	}
	if err := zr.Close(); err != nil {
		return nil, err
	}
	return io.ReadAll(&buf)
}

var BlobPath = func(blobName string) string {
	return filepath.Join(*pathPrefix, blobName)
}

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

type compressedWriter struct {
	ctx        context.Context
	gzipWriter *gzip.Writer
	closer     io.Closer
	label      string
	n          int
}

func NewCompressedBlobStoreWriter(ctx context.Context, w io.Writer, c io.Closer, label string) io.WriteCloser {
	return &compressedWriter{ctx: ctx, gzipWriter: gzip.NewWriter(w), closer: c, label: label, n: 0}
}

func (d *compressedWriter) Write(p []byte) (int, error) {
	start := time.Now()
	_, spn := tracing.StartSpan(d.ctx)
	n, err := d.gzipWriter.Write(p)
	spn.End()
	RecordWriteMetrics(d.label, start, d.n, err)
	d.n += n
	return n, err
}

func (d *compressedWriter) Close() error {
	d.gzipWriter.Close()
	err := d.closer.Close()
	return err
}

type AsyncCloser struct {
	Closer       io.Closer
	ErrorChannel chan error
}

func (a *AsyncCloser) Close() error {
	if err := a.Closer.Close(); err != nil {
		if err := <-a.ErrorChannel; err != nil {
			return err
		}
		return err
	}
	return <-a.ErrorChannel
}

type CustomCloser struct {
	CloseOp func() error
}

func (c *CustomCloser) Close() error {
	return c.CloseOp()
}
