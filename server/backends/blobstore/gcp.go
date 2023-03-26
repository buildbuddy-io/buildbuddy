package blobstore

import (
	"context"
	"flag"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"google.golang.org/api/option"
)

var (
	// GCS flags
	gcsBucket          = flag.String("storage.gcs.bucket", "", "The name of the GCS bucket to store build artifact files in.")
	gcsCredentialsFile = flag.String("storage.gcs.credentials_file", "", "A path to a JSON credentials file that will be used to authenticate to GCS.")
	gcsProjectID       = flag.String("storage.gcs.project_id", "", "The Google Cloud project ID of the project owning the above credentials and GCS bucket.")
)

// GCSBlobStore implements the blobstore API on top of the google cloud storage API.
type GCSBlobStore struct {
	gcsClient    *storage.Client
	bucketHandle *storage.BucketHandle
	projectID    string
}

func NewGCSBlobStore(ctx context.Context, bucketName, projectID string, opts ...option.ClientOption) (*GCSBlobStore, error) {
	gcsClient, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	g := &GCSBlobStore{
		gcsClient: gcsClient,
		projectID: projectID,
	}
	err = g.createBucketIfNotExists(ctx, bucketName)
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
	reader, err := g.bucketHandle.Object(blobPath(blobName)).NewReader(ctx)
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
	recordReadMetrics(gcsLabel, start, b, err)
	return decompress(b, err)
}

func (g *GCSBlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	writer := g.bucketHandle.Object(blobPath(blobName)).NewWriter(ctx)
	defer writer.Close()
	compressedData, err := compress(data)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	ctx, spn := tracing.StartSpan(ctx)
	n, err := writer.Write(compressedData)
	spn.End()
	recordWriteMetrics(gcsLabel, start, n, err)
	return n, err
}

func (g *GCSBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	start := time.Now()
	ctx, spn := tracing.StartSpan(ctx)
	err := g.bucketHandle.Object(blobPath(blobName)).Delete(ctx)
	spn.End()
	recordDeleteMetrics(gcsLabel, start, err)
	if err == storage.ErrObjectNotExist {
		return nil
	}
	return err
}

func (g *GCSBlobStore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	ctx, spn := tracing.StartSpan(ctx)
	_, err := g.bucketHandle.Object(blobPath(blobName)).Attrs(ctx)
	spn.End()
	if err == storage.ErrObjectNotExist {
		return false, nil
	} else if err == nil {
		return true, nil
	} else {
		return false, err
	}
}
