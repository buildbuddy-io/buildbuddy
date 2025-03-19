package gcs

import (
	"context"
	"io"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

var (
	// GCS flags
	gcsBucket          = flag.String("storage.gcs.bucket", "", "The name of the GCS bucket to store build artifact files in.")
	gcsCredentialsFile = flag.String("storage.gcs.credentials_file", "", "A path to a JSON credentials file that will be used to authenticate to GCS.")
	gcsCredentials     = flag.String("storage.gcs.credentials", "", "Credentials in JSON format that will be used to authenticate to GCS.", flag.Secret)
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
	compress     bool
}

func UseGCSBlobStore() bool {
	return *gcsBucket != ""
}

func NewGCSBlobStoreFromFlags(ctx context.Context) (*GCSBlobStore, error) {
	return NewGCSBlobStore(ctx, *gcsBucket, *gcsCredentialsFile, *gcsCredentials, *gcsProjectID, true /*=enableCompression*/)
}

func NewGCSBlobStore(ctx context.Context, bucket, credsFile, creds, projectID string, enableCompression bool) (*GCSBlobStore, error) {
	opts := make([]option.ClientOption, 0)
	if creds != "" && credsFile != "" {
		return nil, status.FailedPreconditionError("GCS credentials should be specified either via file or directly, but not both")
	}
	if credsFile != "" {
		log.Debugf("Found GCS credentials file: %q", credsFile)
		opts = append(opts, option.WithCredentialsFile(credsFile))
	}
	if creds != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(creds)))
	}

	gcsClient, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	g := &GCSBlobStore{
		gcsClient: gcsClient,
		projectID: projectID,
		compress:  enableCompression,
	}
	err = g.createBucketIfNotExists(ctx, bucket)
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
	if err == storage.ErrBucketNotExist {
		return false, nil
	}
	return err == nil, err
}

func (g *GCSBlobStore) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	exists, err := g.bucketExists(ctx, bucketName)
	if err != nil {
		log.Infof("Could not check if GCS bucket exists, skipping bucket creation: %s", err)
	}
	if err == nil && !exists {
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
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	b, err := io.ReadAll(reader)
	spn.End()
	if err := reader.Close(); err != nil {
		log.Errorf("Error closing blobreader: %s", err)
	}
	util.RecordReadMetrics(gcsLabel, start, b, err)
	if g.compress {
		return util.Decompress(b, err)
	} else {
		return b, err
	}
}

func (g *GCSBlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	dataToUpload := data
	if g.compress {
		d, err := util.Compress(data)
		if err != nil {
			return 0, err
		}
		dataToUpload = d
	}

	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()

	doWrite := func() (int, error) {
		start := time.Now()
		writer := g.bucketHandle.Object(blobName).NewWriter(ctx)
		// See https://pkg.go.dev/cloud.google.com/go/storage#Writer,
		// but to avoid unnecessary allocations for blobs << 16MB,
		// ChunkSize should be set before the first write call to 0.
		// This will disable internal buffering and automatic retries.
		blobSize := len(dataToUpload)
		if blobSize < googleapi.DefaultUploadChunkSize {
			writer.ChunkSize = 0
		}
		n, err := writer.Write(dataToUpload)
		if closeErr := writer.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
		util.RecordWriteMetrics(gcsLabel, start, n, err)
		return n, err
	}

	lastErr := status.InternalError("GCS write not run")
	r := retry.New(ctx, &retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2,
		MaxRetries:     0,
	})
	for r.Next() {
		n, err := doWrite()
		if err == nil {
			return n, err
		}
		lastErr = err
	}
	return 0, lastErr
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

// ConditionalWriter is a custom writer for storing expiring artifacts that
// contain already compressed cache bytes. You probably want to use the Writer
// API instead.
//
// If overwriteExisting is false, then calling Commit() on the returned
// interfaces.CommittedWriteCloser may return an AlreadyExistsError indicating
// that an object with the same name already existed and was not overwritten.
func (g *GCSBlobStore) ConditionalWriter(ctx context.Context, blobName string, overwriteExisting bool, customTime time.Time) (interfaces.CommittedWriteCloser, error) {
	ctx, cancel := context.WithCancel(ctx)
	start := time.Now()

	var ow *storage.Writer
	if overwriteExisting {
		ow = g.bucketHandle.Object(blobName).NewWriter(ctx)
	} else {
		ow = g.bucketHandle.Object(blobName).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	}

	// See https://pkg.go.dev/cloud.google.com/go/storage#Writer
	// Always disable buffering in the client for these writes.
	ow.ChunkSize = 0
	ow.ObjectAttrs.CustomTime = customTime

	cwc := ioutil.NewCustomCommitWriteCloser(ow)
	cwc.CommitFn = func(n int64) error {
		err := ow.Close()
		if gerr, ok := err.(*googleapi.Error); ok {
			if gerr.Code == http.StatusPreconditionFailed {
				// Rewrite the error to an AlreadyExistsError that
				// calling code can catch.
				err = status.AlreadyExistsError("blob already exists")
			}
		}
		util.RecordWriteMetrics(gcsLabel, start, int(n), err)
		return err
	}
	cwc.CloseFn = func() error {
		cancel()
		return nil
	}
	return cwc, nil
}

func (g *GCSBlobStore) Writer(ctx context.Context, blobName string) (interfaces.CommittedWriteCloser, error) {
	ctx, cancel := context.WithCancel(ctx)
	ow := g.bucketHandle.Object(blobName).NewWriter(ctx)

	// See https://pkg.go.dev/cloud.google.com/go/storage#Writer
	// Always disable buffering in the client for these writes.
	ow.ChunkSize = 0

	var zw io.WriteCloser
	if g.compress {
		zw = util.NewCompressWriter(ow)
	} else {
		zw = ow
	}
	cwc := ioutil.NewCustomCommitWriteCloser(zw)
	cwc.CommitFn = func(int64) error {
		if compresserCloseErr := zw.Close(); compresserCloseErr != nil {
			cancel() // Don't try to finish the commit op if Close() failed.
			// Canceling the context closes the Writer, so don't call ow.Close().
			return compresserCloseErr
		}
		return ow.Close()
	}
	cwc.CloseFn = func() error {
		cancel()
		return nil
	}
	return cwc, nil
}

type decompressingCloser struct {
	io.ReadCloser
	alwaysClose func() error
}

func (d *decompressingCloser) Close() error {
	var firstError error
	if err := d.ReadCloser.Close(); err != nil {
		firstError = err
	}
	if err := d.alwaysClose(); err != nil && firstError == nil {
		firstError = err
	}
	return firstError
}

func (g *GCSBlobStore) Reader(ctx context.Context, blobName string) (io.ReadCloser, error) {
	reader, err := g.bucketHandle.Object(blobName).NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, status.NotFoundError(err.Error())
		}
		return nil, err
	}
	if g.compress {
		rc, err := util.NewCompressReader(reader)
		if err != nil {
			return nil, err
		}
		return &decompressingCloser{
			ReadCloser:  rc,
			alwaysClose: reader.Close,
		}, nil
	} else {
		return reader, nil
	}
}

func (g *GCSBlobStore) SetBucketCustomTimeTTL(ctx context.Context, ageInDays int64) error {
	ctx, spn := tracing.StartSpan(ctx)
	attrs, err := g.bucketHandle.Attrs(ctx)
	spn.End()
	if err != nil {
		return err
	}

	// If the lifecycle is already set correctly, return nil.
	if ageInDays > 0 {
		for _, rule := range attrs.Lifecycle.Rules {
			if rule.Condition.DaysSinceCustomTime == ageInDays &&
				rule.Action.Type == storage.DeleteAction {
				return nil
			}
		}
	} else {
		if len(attrs.Lifecycle.Rules) == 0 {
			return nil
		}
	}

	// Otherwise, attempt to set the bucket lifecycle.
	lc := storage.Lifecycle{
		Rules: []storage.LifecycleRule{},
	}

	// If ageInDays is 0, send an empty lifecycle rule list in order
	// to clear out existing TTL rules. If ageInDays is > 0, then
	// actually configure a lifecycle rule that will TTL objects.
	if ageInDays > 0 {
		lc.Rules = append(lc.Rules, storage.LifecycleRule{
			Condition: storage.LifecycleCondition{
				DaysSinceCustomTime: ageInDays,
			},
			Action: storage.LifecycleAction{
				Type: storage.DeleteAction,
			},
		})
	}

	ctx, spn = tracing.StartSpan(ctx)
	spn.SetName("Update for GCSCache SetBucketTTL")
	_, err = g.bucketHandle.Update(ctx, storage.BucketAttrsToUpdate{Lifecycle: &lc})
	spn.End()
	return err
}

func (g *GCSBlobStore) UpdateCustomTime(ctx context.Context, blobName string, t time.Time) error {
	ctx, spn := tracing.StartSpan(ctx)
	_, err := g.bucketHandle.Object(blobName).Update(ctx, storage.ObjectAttrsToUpdate{
		CustomTime: t,
	})
	spn.End()

	if gerr, ok := err.(*googleapi.Error); ok {
		if gerr.Code == http.StatusTooManyRequests {
			// Rewrite the error to an AlreadyExistsError that
			// calling code can catch.
			err = status.ResourceExhaustedError("blob atime already updated")
		}
	}
	return err
}
