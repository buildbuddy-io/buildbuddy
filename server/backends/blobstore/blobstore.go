package blobstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/option"

	gstatus "google.golang.org/grpc/status"
)

const (
	// Prometheus BlobstoreTypeLabel values

	diskLabel         = "disk"
	gcsLabel          = "gcs"
	awsS3Label        = "aws_s3"
	bucketWaitTimeout = 10 * time.Second
)

// Returns whatever blobstore is specified in the config.
func GetConfiguredBlobstore(c *config.Configurator) (interfaces.Blobstore, error) {
	log.Debug("Configuring blobstore")
	if c.GetStorageDiskRootDir() != "" {
		log.Debug("Disk blobstore configured")
		return NewDiskBlobStore(c.GetStorageDiskRootDir())
	}
	if gcsConfig := c.GetStorageGCSConfig(); gcsConfig != nil && gcsConfig.Bucket != "" {
		log.Debug("Configuring GCS blobstore")
		opts := make([]option.ClientOption, 0)
		if gcsConfig.CredentialsFile != "" {
			log.Debugf("Found GCS credentials file: %q", gcsConfig.CredentialsFile)
			opts = append(opts, option.WithCredentialsFile(gcsConfig.CredentialsFile))
		}
		return NewGCSBlobStore(gcsConfig.Bucket, gcsConfig.ProjectID, opts...)
	}

	if awsConfig := c.GetStorageAWSS3Config(); awsConfig != nil && awsConfig.Bucket != "" {
		log.Debug("Configuring AWS blobstore")
		return NewAwsS3BlobStore(awsConfig)
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

// A Disk-based blob storage implementation that reads and writes blobs to/from
// files.
type DiskBlobStore struct {
	rootDir string
}

func NewDiskBlobStore(rootDir string) (*DiskBlobStore, error) {
	if err := disk.EnsureDirectoryExists(rootDir); err != nil {
		return nil, err
	}
	return &DiskBlobStore{
		rootDir: rootDir,
	}, nil
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
	rsp, err := ioutil.ReadAll(zr)
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
	return ioutil.ReadAll(&buf)
}

func (d *DiskBlobStore) blobPath(blobName string) (string, error) {
	// Probably could be more careful here but we are generating these ourselves
	// for now.
	if strings.Contains(blobName, "..") {
		return "", fmt.Errorf("blobName (%s) must not contain ../", blobName)
	}
	return filepath.Join(d.rootDir, blobName), nil
}

func (d *DiskBlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return 0, err
	}

	compressedData, err := compress(data)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	n, err := disk.WriteFile(ctx, fullPath, compressedData)
	recordWriteMetrics(diskLabel, start, n, err)
	return n, err
}

func (d *DiskBlobStore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	b, err := disk.ReadFile(ctx, fullPath)
	recordReadMetrics(diskLabel, start, b, err)
	return decompress(b, err)
}

func (d *DiskBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return err
	}
	start := time.Now()
	err = disk.DeleteFile(ctx, fullPath)
	recordDeleteMetrics(diskLabel, start, err)
	return err
}

func (d *DiskBlobStore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return false, err
	}
	return disk.FileExists(fullPath)
}

// GCSBlobStore implements the blobstore API on top of the google cloud storage API.
type GCSBlobStore struct {
	gcsClient    *storage.Client
	bucketHandle *storage.BucketHandle
	projectID    string
}

func NewGCSBlobStore(bucketName, projectID string, opts ...option.ClientOption) (*GCSBlobStore, error) {
	ctx := context.Background()
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

func (g *GCSBlobStore) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	if _, err := g.gcsClient.Bucket(bucketName).Attrs(ctx); err != nil {
		log.Infof("Creating storage bucket: %s", bucketName)
		g.bucketHandle = g.gcsClient.Bucket(bucketName)
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
	b, err := ioutil.ReadAll(reader)
	recordReadMetrics(gcsLabel, start, b, err)
	return decompress(b, err)
}

func (g *GCSBlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	writer := g.bucketHandle.Object(blobName).NewWriter(ctx)
	defer writer.Close()
	compressedData, err := compress(data)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	n, err := writer.Write(compressedData)
	recordWriteMetrics(gcsLabel, start, n, err)
	return n, err
}

func (g *GCSBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	start := time.Now()
	err := g.bucketHandle.Object(blobName).Delete(ctx)
	recordDeleteMetrics(gcsLabel, start, err)
	return err
}

func (g *GCSBlobStore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	_, err := g.bucketHandle.Object(blobName).Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return false, nil
	} else if err == nil {
		return true, nil
	} else {
		return false, err
	}
}

// AWS stuff
type AwsS3BlobStore struct {
	s3         *s3.S3
	bucket     *string
	downloader *s3manager.Downloader
	uploader   *s3manager.Uploader
}

func NewAwsS3BlobStore(awsConfig *config.AwsS3Config) (*AwsS3BlobStore, error) {
	ctx := context.Background()

	config := &aws.Config{
		Region: aws.String(awsConfig.Region),
	}
	// See https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials
	if awsConfig.CredentialsProfile != "" {
		log.Debugf("AWS blobstore credentials profile found: %q", awsConfig.CredentialsProfile)
		config.Credentials = credentials.NewSharedCredentials("", awsConfig.CredentialsProfile)
	}
	if awsConfig.StaticCredentialsID != "" && awsConfig.StaticCredentialsSecret != "" {
		log.Debugf("AWS blobstore static credentials found: %q", awsConfig.StaticCredentialsID)
		config.Credentials = credentials.NewStaticCredentials(awsConfig.StaticCredentialsID, awsConfig.StaticCredentialsSecret, awsConfig.StaticCredentialsToken)
	}
	if awsConfig.Endpoint != "" {
		log.Debugf("AWS blobstore endpoint found: %q", awsConfig.Endpoint)
		config.Endpoint = aws.String(awsConfig.Endpoint)
	}
	if awsConfig.DisableSSL {
		log.Debug("AWS blobstore disabling SSL")
		config.DisableSSL = aws.Bool(awsConfig.DisableSSL)
	}
	if awsConfig.S3ForcePathStyle {
		log.Debug("AWS blobstore forcing path style")
		config.S3ForcePathStyle = aws.Bool(awsConfig.S3ForcePathStyle)
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}
	log.Debug("AWS blobstore session created")

	// Create S3 service client
	svc := s3.New(sess)
	log.Debug("AWS blobstore service client created")

	awsBlobStore := &AwsS3BlobStore{
		s3:         svc,
		bucket:     aws.String(awsConfig.Bucket),
		downloader: s3manager.NewDownloader(sess),
		uploader:   s3manager.NewUploader(sess),
	}

	// S3 access points can't modify or delete buckets
	// https://github.com/awsdocs/amazon-s3-developer-guide/blob/master/doc_source/access-points.md
	const s3AccessPointPrefix = "arn:aws:s3"
	if strings.HasPrefix(awsConfig.Bucket, s3AccessPointPrefix) {
		log.Infof("Encountered an S3 access point %s...not creating bucket", awsConfig.Bucket)
	} else {
		if err := awsBlobStore.createBucketIfNotExists(ctx, awsConfig.Bucket); err != nil {
			return nil, err
		}
	}
	log.Debug("AWS blobstore configured")
	return awsBlobStore, nil
}

func (a *AwsS3BlobStore) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	// HeadBucket call will return 404 or 403
	log.Debugf("Checking if AWS blobstore bucket %q exists", bucketName)
	if _, err := a.s3.HeadBucketWithContext(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)}); err != nil {
		aerr := err.(awserr.Error)
		// AWS returns codes as strings
		// https://github.com/aws/aws-sdk-go/blob/master/service/s3/s3manager/bucket_region_test.go#L70
		if aerr.Code() != "NotFound" {
			return err
		}
		log.Debugf("AWS blobstore bucket %q does not exists", bucketName)

		log.Infof("Creating storage bucket: %s", bucketName)
		if _, err := a.s3.CreateBucketWithContext(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)}); err != nil {
			return err
		}
		log.Debug("Waiting until AWS Bucket exists")
		ctx, cancel := context.WithTimeout(ctx, bucketWaitTimeout)
		defer cancel()
		return a.s3.WaitUntilBucketExistsWithContext(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
	}
	log.Debugf("AWS blobstore bucket %q already exists", bucketName)
	return nil
}

func (a *AwsS3BlobStore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	start := time.Now()
	b, err := a.download(ctx, blobName)
	recordReadMetrics(awsS3Label, start, b, err)
	return decompress(b, err)
}

func (a *AwsS3BlobStore) download(ctx context.Context, blobName string) ([]byte, error) {
	buff := &aws.WriteAtBuffer{}

	_, err := a.downloader.DownloadWithContext(ctx, buff, &s3.GetObjectInput{
		Bucket: a.bucket,
		Key:    aws.String(blobName),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == "NoSuchKey" {
				return nil, status.NotFoundError(err.Error())
			}
		}
	}

	return buff.Bytes(), nil
}

func (a *AwsS3BlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	compressedData, err := compress(data)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	n, err := a.upload(ctx, blobName, compressedData)
	recordWriteMetrics(awsS3Label, start, n, err)
	return n, err
}

func (a *AwsS3BlobStore) upload(ctx context.Context, blobName string, compressedData []byte) (int, error) {
	uploadParams := &s3manager.UploadInput{
		Bucket: a.bucket,
		Key:    aws.String(blobName),
		Body:   bytes.NewReader(compressedData),
	}
	if _, err := a.uploader.UploadWithContext(ctx, uploadParams); err != nil {
		return -1, err
	}
	return len(compressedData), nil
}

func (a *AwsS3BlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	start := time.Now()
	err := a.delete(ctx, blobName)
	recordDeleteMetrics(awsS3Label, start, err)
	return err
}

func (a *AwsS3BlobStore) delete(ctx context.Context, blobName string) error {
	deleteParams := &s3.DeleteObjectInput{
		Bucket: a.bucket,
		Key:    aws.String(blobName),
	}

	if _, err := a.s3.DeleteObjectWithContext(ctx, deleteParams); err != nil {
		return err
	}

	return a.s3.WaitUntilObjectNotExistsWithContext(ctx, &s3.HeadObjectInput{
		Bucket: a.bucket,
		Key:    aws.String(blobName),
	})
}

func (a *AwsS3BlobStore) BlobExists(ctx context.Context, blobName string) (bool, error) {

	params := &s3.HeadObjectInput{
		Bucket: a.bucket,
		Key:    aws.String(blobName),
	}

	if _, err := a.s3.HeadObjectWithContext(ctx, params); err != nil {
		return false, err
	}

	return true, nil
}
