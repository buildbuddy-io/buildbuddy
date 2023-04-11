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

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"

	gstatus "google.golang.org/grpc/status"
)

var (
	pathPrefix = flag.String("storage.path_prefix", "", "The prefix directory to store all blobs in")
)

var newCompressWriter = gzip.NewWriter
var newCompressReader = gzip.NewReader

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
	zr, err := newCompressReader(&buf)
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
	zr := newCompressWriter(&buf)
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
	ctx       context.Context
	committer interfaces.Committer
	zw        io.WriteCloser
	closer    io.Closer
	label     string
	n         int
}

func NewCompressedBlobStoreWriter(
	ctx context.Context,
	committer interfaces.Committer,
	writer io.Writer,
	closer io.Closer,
	label string,
) interfaces.CommittedWriteCloser {
	return &compressedWriter{
		ctx: ctx, committer: committer,
		zw:     newCompressWriter(writer),
		closer: closer,
		label:  label,
		n:      0,
	}
}

func (c *compressedWriter) Commit() error {
	c.zw.Close()
	return c.committer.Commit()
}

func (c *compressedWriter) Write(p []byte) (int, error) {
	//Unnecessary without the RecordWriteMetrics call
	// start := time.Now()
	_, spn := tracing.StartSpan(c.ctx)
	n, err := c.zw.Write(p)
	spn.End()
	// Omit these metrics for now, since they differ fundamentally from the other
	// write metrics, which both separate out the compression step and track the
	// full blob upload operation as opposed to just the streaming of one chunk.
	// RecordWriteMetrics(c.label, start, n, err)
	c.n += n
	return n, err
}

func (c *compressedWriter) Close() error {
	return c.closer.Close()
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

type CustomCommitter struct {
	CommitOp func() error
}

func (c *CustomCommitter) Commit() error {
	return c.CommitOp()
}

type CustomCloser struct {
	CloseOp func() error
}

func (c *CustomCloser) Close() error {
	return c.CloseOp()
}
