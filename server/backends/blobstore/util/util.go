package util

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"io"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/prometheus/client_golang/prometheus"

	gstatus "google.golang.org/grpc/status"
)

var (
	pathPrefix      = flag.String("storage.path_prefix", "", "The prefix directory to store all blobs in")
	zstdCompression = flag.Bool("storage.zstd_compression", false, "If true, compress newly written blobs with zstd instead of gzip. Reads auto-detect the format, so only enable this once all readers of the blobstore support zstd.")
)

const (
	// How much data the streaming zstd writer buffers before compressing and
	// writing a frame. Larger buffers compress better; this much memory (plus
	// a compression bound) is pooled per concurrent blob write.
	zstdWriteBufSizeBytes = 1024 * 1024 // 1MB
)

var (
	// gzipMagic is the 2-byte magic number that prefixes all gzip streams.
	gzipMagic = []byte{0x1f, 0x8b}
	// zstdMagic is the 4-byte magic number that prefixes all zstd frames.
	zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

	zstdWriteBufPool = bytebufferpool.VariableSize(int(compression.ZstdCompressBound(zstdWriteBufSizeBytes)))
)

// NewCompressWriter returns a writer that compresses data written to it and
// writes the compressed output to w. Closing the returned writer flushes any
// buffered data but does not close w. The compression format is zstd if
// storage.zstd_compression is enabled, gzip otherwise; readers auto-detect
// the format, so the flag can be toggled without migrating existing blobs.
func NewCompressWriter(w io.Writer) io.WriteCloser {
	if *zstdCompression {
		zw, err := compression.NewZstdCompressingWriter(w, zstdWriteBufPool, zstdWriteBufSizeBytes)
		if err == nil {
			return zw
		}
		// Only reachable if zstdWriteBufSizeBytes is misconfigured; gzip
		// output is always safe since readers auto-detect the format.
		log.Errorf("Failed to create zstd blobstore writer, falling back to gzip: %s", err)
	}
	return gzip.NewWriter(w)
}

// NewCompressReader returns a reader that decompresses data read from r,
// detecting the compression format (zstd or gzip) from the stream's magic
// bytes. Data in neither format is passed through as-is, for compatibility
// with blobs written before compression was enabled. Closing the returned
// reader releases decompression resources but does not close r.
func NewCompressReader(r io.Reader) (io.ReadCloser, error) {
	br := bufio.NewReader(r)
	header, err := br.Peek(len(zstdMagic))
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	if bytes.HasPrefix(header, zstdMagic) {
		return compression.NewZstdDecompressingReader(io.NopCloser(br))
	}
	if bytes.HasPrefix(header, gzipMagic) {
		return gzip.NewReader(br)
	}
	// Compatibility hack: this is probably an uncompressed record written
	// before we were compressing. Just read it as-is.
	return io.NopCloser(br), nil
}

func Decompress(in []byte, err error) ([]byte, error) {
	if err != nil {
		return in, err
	}
	if bytes.HasPrefix(in, zstdMagic) {
		return compression.DecompressZstd(nil, in)
	}
	if !bytes.HasPrefix(in, gzipMagic) {
		// Compatibility hack: this is probably an uncompressed record
		// written before we were compressing. Just read it as-is.
		return in, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	var buffer bytes.Buffer
	_, err = io.Copy(&buffer, zr)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func Compress(in []byte) ([]byte, error) {
	if *zstdCompression {
		return compression.CompressZstd(nil, in), nil
	}
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
		metrics.StatusLabel:        gstatus.Code(err).String(),
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

func RecordReadMetrics(typeLabel string, startTime time.Time, size int, err error) {
	duration := time.Since(startTime)
	metrics.BlobstoreReadCount.With(prometheus.Labels{
		metrics.StatusLabel:        gstatus.Code(err).String(),
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
		metrics.StatusLabel:        gstatus.Code(err).String(),
		metrics.BlobstoreTypeLabel: typeLabel,
	}).Inc()
	// Don't track duration if there's an error, but do track
	// count (above) so we can measure failure rates.
	if err != nil {
		return
	}
	metrics.BlobstoreDeleteDurationUsec.With(prometheus.Labels{
		metrics.BlobstoreTypeLabel: typeLabel,
	}).Observe(float64(duration.Microseconds()))
}

func RecordExistsMetrics(typeLabel string, startTime time.Time, err error) {
	duration := time.Since(startTime)
	metrics.BlobstoreExistsCount.With(prometheus.Labels{
		metrics.StatusLabel:        gstatus.Code(err).String(),
		metrics.BlobstoreTypeLabel: typeLabel,
	})
	// Don't track duration if there's an error, but do track
	// count (above) so we can measure failure rates.
	if err != nil {
		return
	}
	metrics.BlobstoreExistsDurationUsec.With(prometheus.Labels{
		metrics.BlobstoreTypeLabel: typeLabel,
	}).Observe(float64(duration.Microseconds()))
}
