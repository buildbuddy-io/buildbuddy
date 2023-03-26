package blobstore

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/option"

	gstatus "google.golang.org/grpc/status"
)

var pathPrefix = flag.String("storage.path_prefix", "", "The prefix directory to store all blobs in")

const (
	// Prometheus BlobstoreTypeLabel values
	diskLabel         = "disk"
	gcsLabel          = "gcs"
	awsS3Label        = "aws_s3"
	azureLabel        = "azure"
	bucketWaitTimeout = 10 * time.Second
)

// Returns whatever blobstore is specified in the config.
func GetConfiguredBlobstore(env environment.Env) (interfaces.Blobstore, error) {
	log.Debug("Configuring blobstore")
	ctx := env.GetServerContext()
	if *gcsBucket != "" {
		log.Debug("Configuring GCS blobstore")
		opts := make([]option.ClientOption, 0)
		if *gcsCredentialsFile != "" {
			log.Debugf("Found GCS credentials file: %q", *gcsCredentialsFile)
			opts = append(opts, option.WithCredentialsFile(*gcsCredentialsFile))
		}
		return NewGCSBlobStore(ctx, *gcsBucket, *gcsProjectID, opts...)
	}
	if *awsS3Bucket != "" {
		log.Debug("Configuring AWS blobstore")
		return NewAwsS3BlobStore(ctx)
	}
	if *azureContainerName != "" {
		log.Debug("Configuring Azure blobstore")
		return NewAzureBlobStore(ctx, *azureContainerName, *azureAccountName, *azureAccountKey)
	}
	if *rootDirectory != "" {
		log.Debug("Disk blobstore configured")
		return NewDiskBlobStore(*rootDirectory)
	}
	return nil, fmt.Errorf("No storage backend configured -- please specify at least one in the config")
}

func recordWriteMetrics(typeLabel string, startTime time.Time, size int, err error) {
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

func recordReadMetrics(typeLabel string, startTime time.Time, b []byte, err error) {
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

func recordDeleteMetrics(typeLabel string, startTime time.Time, err error) {
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

func decompress(in []byte, err error) ([]byte, error) {
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

func compress(in []byte) ([]byte, error) {
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

func blobPath(blobName string) string {
	return filepath.Join(*pathPrefix, blobName)
}
