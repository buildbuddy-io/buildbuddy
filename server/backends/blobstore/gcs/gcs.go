package gcs

import (
	"context"
	"flag"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"google.golang.org/api/option"
)

var (
	// GCS flags
	gcsBucket          = flag.String("storage.gcs.bucket", "", "The name of the GCS bucket to store build artifact files in.")
	gcsCredentialsFile = flag.String("storage.gcs.credentials_file", "", "A path to a JSON credentials file that will be used to authenticate to GCS.")
	gcsCredentials     = flagutil.New("storage.gcs.credentials", "", "Credentials in JSON format that will be used to authenticate to GCS.", flagutil.SecretTag)
	gcsProjectID       = flag.String("storage.gcs.project_id", "", "The Google Cloud project ID of the project owning the above credentials and GCS bucket.")
)

const (
	// Prometheus BlobstoreTypeLabel values
	gcsLabel = "gcs"
)

// GCSBlobStore implements the blobstore API on top of the google cloud storage API.
type GCSBlobStore struct {
	gcsClient    *storage.Client
	bucketHandle *storage.BucketHandle
	projectID    string
}

func UseGCSBlobStore() bool {
	return *gcsBucket != ""
}

func NewGCSBlobStore(ctx context.Context) (*GCSBlobStore, error) {
	opts := make([]option.ClientOption, 0)
	if *gcsCredentials != "" && *gcsCredentialsFile != "" {
		return nil, status.FailedPreconditionError("GCS credentials should be specified either via file or directly, but not both")
	}
	if *gcsCredentialsFile != "" {
		log.Debugf("Found GCS credentials file: %q", *gcsCredentialsFile)
		opts = append(opts, option.WithCredentialsFile(*gcsCredentialsFile))
	}
	if *gcsCredentials != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(*gcsCredentials)))
	}

	gcsClient, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	g := &GCSBlobStore{
		gcsClient: gcsClient,
		projectID: *gcsProjectID,
	}
	err = g.createBucketIfNotExists(ctx, *gcsBucket)
	if err != nil {
		return nil, err
	}
	log.Debug("GCS blobstore configured")
	return g, nil
}

func (g *GCSBlobStore) bucketExists(ctx context.Context, bucketName string) (bool, error) {
	ctx, spn := tracing.StartSpan(ctx)
	_, err := g.gcsClient.Bucket(bucketName).Attrs(ctx)
	spn.End()
	return err == nil, err
}

func (g *GCSBlobStore) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	if exists, _ := g.bucketExists(ctx, bucketName); !exists {
		log.Infof("Creating storage bucket: %s", bucketName)
		g.bucketHandle = g.gcsClient.Bucket(bucketName)
		ctx, spn := tracing.StartSpan(ctx)
		defer spn.End()
		return g.bucketHandle.Create(ctx, g.projectID, nil)
	}
	g.bucketHandle = g.gcsClient.Bucket(bucketName)
	return nil
}

func (g *GCSBlobStore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	reader, err := g.bucketHandle.Object(blobName).NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, status.NotFoundError(err.Error())
		}
		return nil, err
	}
	start := time.Now()
	ctx, spn := tracing.StartSpan(ctx)
	b, err := io.ReadAll(reader)
	spn.End()
	util.RecordReadMetrics(gcsLabel, start, b, err)
	return util.Decompress(b, err)
}

func (g *GCSBlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	writer := g.bucketHandle.Object(blobName).NewWriter(ctx)
	defer writer.Close()
	compressedData, err := util.Compress(data)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	ctx, spn := tracing.StartSpan(ctx)
	n, err := writer.Write(compressedData)
	spn.End()
	util.RecordWriteMetrics(gcsLabel, start, n, err)
	return n, err
}

func (g *GCSBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	start := time.Now()
	ctx, spn := tracing.StartSpan(ctx)
	err := g.bucketHandle.Object(blobName).Delete(ctx)
	spn.End()
	util.RecordDeleteMetrics(gcsLabel, start, err)
	if err == storage.ErrObjectNotExist {
		return nil
	}
	return err
}

func (g *GCSBlobStore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	ctx, spn := tracing.StartSpan(ctx)
	_, err := g.bucketHandle.Object(blobName).Attrs(ctx)
	spn.End()
	if err == storage.ErrObjectNotExist {
		return false, nil
	} else if err == nil {
		return true, nil
	} else {
		return false, err
	}
}

func (g *GCSBlobStore) Writer(ctx context.Context, blobName string) (interfaces.CommittedWriteCloser, error) {
	ctx, cancel := context.WithCancel(ctx)
	bw := g.bucketHandle.Object(blobName).NewWriter(ctx)

	zw := util.NewCompressWriter(bw)
	cwc := ioutil.NewCustomCommitWriteCloser(zw)
	cwc.CommitFn = func(int64) error {
		if compresserCloseErr := zw.Close(); compresserCloseErr != nil {
			cancel() // Don't try to finish the commit op if Close() failed.
			// Canceling the context closes the Writer, so don't call bw.Close().
			return compresserCloseErr
		}
		return bw.Close()
	}
	cwc.CloseFn = func() error {
		cancel()
		return nil
	}
	return cwc, nil
}
