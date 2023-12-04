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
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/prometheus/client_golang/prometheus"

	gstatus "google.golang.org/grpc/status"
)

var pathPrefix = flag.String("storage.path_prefix", "", "The prefix directory to store all blobs in")

func NewCompressWriter(w io.Writer) io.WriteCloser {
	return gzip.NewWriter(w)
}

func NewCompressReader(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

func Decompress(r io.Reader) ([]byte, error) {
	var wrappedReader io.Reader
	zr, err := NewCompressReader(r)
	if err != nil {
		if err != gzip.ErrHeader {
			return nil, err
		}
		// Compatibility hack: if we got a header error it means this
		// is probably an uncompressed record written before we were
		// compressing. Just read it as-is.
		wrappedReader = r
	} else {
		wrappedReader = zr
	}
	defer zr.Close()
	return ioutil.ReadAll(wrappedReader)
}

func Compress(in []byte) ([]byte, error) {
	var buf bytes.Buffer
	zr := NewCompressWriter(&buf)
	if _, err := zr.Write(in); err != nil {
		return nil, err
	}
	if err := zr.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// prefixBlobstore implements interfaces.Blobstore, is a wrapper around
// an existing blobstore that prefixes all blob names with `storage.path_prefix`.
type prefixBlobstore struct {
	blobstore interfaces.Blobstore
	prefix    string
}

func NewDefaultPrefixBlobstore(b interfaces.Blobstore) *prefixBlobstore {
	return NewPrefixBlobstore(b, *pathPrefix)
}

// NewPrefixBlobstore returns a new prefixBlobstore that wraps the given
// blobstore and prefixes all blob names with the given prefix.
// Intent to support testing only, you probably want to use
// NewDefaultPrefixBlobstore instead.
func NewPrefixBlobstore(b interfaces.Blobstore, prefix string) *prefixBlobstore {
	return &prefixBlobstore{b, prefix}
}

func (p *prefixBlobstore) blobPath(blobName string) string {
	return filepath.Join(p.prefix, blobName)
}

func (p *prefixBlobstore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	return p.blobstore.BlobExists(ctx, p.blobPath(blobName))
}

func (p *prefixBlobstore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	return p.blobstore.ReadBlob(ctx, p.blobPath(blobName))
}

func (p *prefixBlobstore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	return p.blobstore.WriteBlob(ctx, p.blobPath(blobName), data)
}

func (p *prefixBlobstore) DeleteBlob(ctx context.Context, blobName string) error {
	return p.blobstore.DeleteBlob(ctx, p.blobPath(blobName))
}

func (p *prefixBlobstore) Writer(ctx context.Context, blobName string) (interfaces.CommittedWriteCloser, error) {
	return p.blobstore.Writer(ctx, p.blobPath(blobName))
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

func RecordReadMetrics(typeLabel string, duration time.Duration, size int64, err error) {
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
