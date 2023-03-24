package blobstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/option"

	gstatus "google.golang.org/grpc/status"
)

var (
	pathPrefix = flag.String("storage.path_prefix", "", "The prefix directory to store all blobs in")

	// Disk flags
	rootDirectory = flag.String("storage.disk.root_directory", "/tmp/buildbuddy", "The root directory to store all blobs in, if using disk based storage.")
	useV2Layout   = flag.Bool("storage.disk.use_v2_layout", false, "If enabled, files will be stored using the v2 layout. See disk_cache.MigrateToV2Layout for a description.")

	// GCS flags
	gcsBucket          = flag.String("storage.gcs.bucket", "", "The name of the GCS bucket to store build artifact files in.")
	gcsCredentialsFile = flag.String("storage.gcs.credentials_file", "", "A path to a JSON credentials file that will be used to authenticate to GCS.")
	gcsProjectID       = flag.String("storage.gcs.project_id", "", "The Google Cloud project ID of the project owning the above credentials and GCS bucket.")

	// AWS S3 flags
	awsS3Region                   = flag.String("storage.aws_s3.region", "", "The AWS region.")
	awsS3Bucket                   = flag.String("storage.aws_s3.bucket", "", "The AWS S3 bucket to store files in.")
	awsS3CredentialsProfile       = flag.String("storage.aws_s3.credentials_profile", "", "A custom credentials profile to use.")
	awsS3WebIdentityTokenFilePath = flag.String("storage.aws_s3.web_identity_token_file", "", "The file path to the web identity token file.")
	awsS3RoleARN                  = flag.String("storage.aws_s3.role_arn", "", "The role ARN to use for web identity auth.")
	awsS3RoleSessionName          = flag.String("storage.aws_s3.role_session_name", "", "The role session name to use for web identity auth.")
	awsS3Endpoint                 = flag.String("storage.aws_s3.endpoint", "", "The AWS endpoint to use, useful for configuring the use of MinIO.")
	awsS3StaticCredentialsID      = flag.String("storage.aws_s3.static_credentials_id", "", "Static credentials ID to use, useful for configuring the use of MinIO.")
	awsS3StaticCredentialsSecret  = flagutil.New("storage.aws_s3.static_credentials_secret", "", "Static credentials secret to use, useful for configuring the use of MinIO.", flagutil.SecretTag)
	awsS3StaticCredentialsToken   = flag.String("storage.aws_s3.static_credentials_token", "", "Static credentials token to use, useful for configuring the use of MinIO.")
	awsS3DisableSSL               = flag.Bool("storage.aws_s3.disable_ssl", false, "Disables the use of SSL, useful for configuring the use of MinIO.")
	awsS3ForcePathStyle           = flag.Bool("storage.aws_s3.s3_force_path_style", false, "Force path style urls for objects, useful for configuring the use of MinIO.")

	// Azure flags
	azureAccountName   = flag.String("storage.azure.account_name", "", "The name of the Azure storage account")
	azureAccountKey    = flagutil.New("storage.azure.account_key", "", "The key for the Azure storage account", flagutil.SecretTag)
	azureContainerName = flag.String("storage.azure.container_name", "", "The name of the Azure storage container")
)

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

func (d *DiskBlobStore) blobPath(blobName string) (string, error) {
	// Probably could be more careful here but we are generating these ourselves
	// for now.
	if strings.Contains(blobName, "..") {
		return "", fmt.Errorf("blobName (%s) must not contain ../", blobName)
	}
	return filepath.Join(d.rootDir, blobPath(blobName)), nil
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
	ctx, spn := tracing.StartSpan(ctx)
	n, err := disk.WriteFile(ctx, fullPath, compressedData)
	spn.End()
	recordWriteMetrics(diskLabel, start, n, err)
	return n, err
}

func (d *DiskBlobStore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	ctx, spn := tracing.StartSpan(ctx)
	b, err := disk.ReadFile(ctx, fullPath)
	spn.End()
	recordReadMetrics(diskLabel, start, b, err)
	return decompress(b, err)
}

func (d *DiskBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return err
	}
	start := time.Now()
	ctx, spn := tracing.StartSpan(ctx)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if fileInfo.IsDir() {
		err = os.RemoveAll(fullPath)
	} else {
		err = disk.DeleteFile(ctx, fullPath)
	}
	spn.End()
	recordDeleteMetrics(diskLabel, start, err)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (d *DiskBlobStore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return false, err
	}
	return disk.FileExists(ctx, fullPath)
}

func blobPath(blobName string) string {
	return filepath.Join(*pathPrefix, blobName)
}

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

// AWS stuff
type AwsS3BlobStore struct {
	s3         *s3.S3
	bucket     *string
	downloader *s3manager.Downloader
	uploader   *s3manager.Uploader
}

func NewAwsS3BlobStore(ctx context.Context) (*AwsS3BlobStore, error) {
	config := &aws.Config{
		Region: aws.String(*awsS3Region),
	}
	// See https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials
	if *awsS3CredentialsProfile != "" {
		log.Debugf("AWS blobstore credentials profile found: %q", *awsS3CredentialsProfile)
		config.Credentials = credentials.NewSharedCredentials("", *awsS3CredentialsProfile)
	}
	if *awsS3StaticCredentialsID != "" && *awsS3StaticCredentialsSecret != "" {
		log.Debugf("AWS blobstore static credentials found: %q", *awsS3StaticCredentialsID)
		config.Credentials = credentials.NewStaticCredentials(*awsS3StaticCredentialsID, *awsS3StaticCredentialsSecret, *awsS3StaticCredentialsToken)
	}
	if *awsS3Endpoint != "" {
		log.Debugf("AWS blobstore endpoint found: %q", *awsS3Endpoint)
		config.Endpoint = aws.String(*awsS3Endpoint)
	}
	if *awsS3DisableSSL {
		log.Debug("AWS blobstore disabling SSL")
		config.DisableSSL = aws.Bool(*awsS3DisableSSL)
	}
	if *awsS3ForcePathStyle {
		log.Debug("AWS blobstore forcing path style")
		config.S3ForcePathStyle = aws.Bool(*awsS3ForcePathStyle)
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}
	log.Debug("AWS blobstore session created")

	if *awsS3WebIdentityTokenFilePath != "" {
		config.Credentials = credentials.NewCredentials(stscreds.NewWebIdentityRoleProvider(sts.New(sess), *awsS3RoleARN, *awsS3RoleSessionName, *awsS3WebIdentityTokenFilePath))
		log.Debugf("AWS web identity credentials set (%s, %s, %s)", *awsS3RoleARN, *awsS3RoleSessionName, *awsS3WebIdentityTokenFilePath)
	}

	// Create S3 service client
	svc := s3.New(sess)
	log.Debug("AWS blobstore service client created")

	awsBlobStore := &AwsS3BlobStore{
		s3:         svc,
		bucket:     aws.String(*awsS3Bucket),
		downloader: s3manager.NewDownloader(sess),
		uploader:   s3manager.NewUploader(sess),
	}

	// S3 access points can't modify or delete buckets
	// https://github.com/awsdocs/amazon-s3-developer-guide/blob/master/doc_source/access-points.md
	const s3AccessPointPrefix = "arn:aws:s3"
	if strings.HasPrefix(*awsS3Bucket, s3AccessPointPrefix) {
		log.Infof("Encountered an S3 access point %s...not creating bucket", *awsS3Bucket)
	} else {
		if err := awsBlobStore.createBucketIfNotExists(ctx, *awsS3Bucket); err != nil {
			return nil, err
		}
	}
	log.Debug("AWS blobstore configured")
	return awsBlobStore, nil
}

func (a *AwsS3BlobStore) bucketExists(ctx context.Context, bucketName string) (bool, error) {
	ctx, spn := tracing.StartSpan(ctx)
	_, err := a.s3.HeadBucketWithContext(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	spn.End()
	if err == nil {
		return true, nil
	}
	aerr := err.(awserr.Error)
	// AWS returns codes as strings
	// https://github.com/aws/aws-sdk-go/blob/master/service/s3/s3manager/bucket_region_test.go#L70
	if aerr.Code() != "NotFound" {
		return false, err
	}
	return false, nil
}

func (a *AwsS3BlobStore) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	// HeadBucket call will return 404 or 403
	log.Debugf("Checking if AWS blobstore bucket %q exists", bucketName)
	if exists, err := a.bucketExists(ctx, bucketName); err != nil {
		return err
	} else if !exists {
		log.Debugf("AWS blobstore bucket %q does not exists", bucketName)

		log.Infof("Creating storage bucket: %s", bucketName)
		ctx, spn := tracing.StartSpan(ctx)
		defer spn.End()
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

	ctx, spn := tracing.StartSpan(ctx)
	_, err := a.downloader.DownloadWithContext(ctx, buff, &s3.GetObjectInput{
		Bucket: a.bucket,
		Key:    aws.String(blobPath(blobName)),
	})
	spn.End()

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
		Key:    aws.String(blobPath(blobName)),
		Body:   bytes.NewReader(compressedData),
	}
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
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
		Key:    aws.String(blobPath(blobName)),
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
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
		Key:    aws.String(blobPath(blobName)),
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	_, err := a.s3.HeadObjectWithContext(ctx, params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// Azure blobstore

const azureURLTemplate = "https://%s.blob.core.windows.net/%s"

// GCSBlobStore implements the blobstore API on top of the google cloud storage API.
type AzureBlobStore struct {
	credential    *azblob.SharedKeyCredential
	containerName string
	containerURL  *azblob.ContainerURL
}

func NewAzureBlobStore(ctx context.Context, containerName, accountName, accountKey string) (*AzureBlobStore, error) {
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	portalURL, err := url.Parse(fmt.Sprintf(azureURLTemplate, accountName, containerName))
	if err != nil {
		return nil, err
	}
	containerURL := azblob.NewContainerURL(*portalURL, pipeline)
	z := &AzureBlobStore{
		credential:    credential,
		containerName: containerName,
		containerURL:  &containerURL,
	}
	if err := z.createContainerIfNotExists(ctx); err != nil {
		return nil, err
	}
	log.Debugf("Azure blobstore configured (container: %q)", containerName)
	return z, nil
}

func (z *AzureBlobStore) isAzureError(err error, code azblob.ServiceCodeType) bool {
	if serr, ok := err.(azblob.StorageError); ok {
		if serr.ServiceCode() == code {
			return true
		}
	}
	return false
}

func (z *AzureBlobStore) containerExists(ctx context.Context) (bool, error) {
	ctx, spn := tracing.StartSpan(ctx)
	_, err := z.containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{})
	spn.End()
	if err == nil {
		return true, nil
	}
	if !z.isAzureError(err, azblob.ServiceCodeContainerNotFound) {
		return false, err
	}
	return false, nil
}

func (z *AzureBlobStore) createContainerIfNotExists(ctx context.Context) error {
	if exists, err := z.containerExists(ctx); err != nil {
		return err
	} else if !exists {
		ctx, spn := tracing.StartSpan(ctx)
		defer spn.End()
		_, err = z.containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		return err
	}
	return nil
}

func (z *AzureBlobStore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	blobURL := z.containerURL.NewBlockBlobURL(blobPath(blobName))
	response, err := blobURL.Download(ctx, 0 /*=offset*/, azblob.CountToEnd, azblob.BlobAccessConditions{}, false /*=rangeGetContentMD5*/, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		if z.isAzureError(err, azblob.ServiceCodeBlobNotFound) {
			return nil, status.NotFoundError(err.Error())
		}
		return nil, err
	}

	start := time.Now()
	readCloser := response.Body(azblob.RetryReaderOptions{})
	defer readCloser.Close()
	ctx, spn := tracing.StartSpan(ctx)
	b, err := io.ReadAll(readCloser)
	spn.End()
	if err != nil {
		return nil, err
	}
	recordReadMetrics(azureLabel, start, b, err)
	return decompress(b, err)
}

func (z *AzureBlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	compressedData, err := compress(data)
	if err != nil {
		return 0, err
	}
	n := len(compressedData)
	start := time.Now()
	blobURL := z.containerURL.NewBlockBlobURL(blobPath(blobName))
	ctx, spn := tracing.StartSpan(ctx)
	_, err = azblob.UploadBufferToBlockBlob(ctx, compressedData, blobURL, azblob.UploadToBlockBlobOptions{})
	spn.End()
	recordWriteMetrics(azureLabel, start, n, err)
	return n, err
}

func (z *AzureBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	start := time.Now()
	blobURL := z.containerURL.NewBlockBlobURL(blobPath(blobName))
	ctx, spn := tracing.StartSpan(ctx)
	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	spn.End()
	recordDeleteMetrics(azureLabel, start, err)
	return err
}

func (z *AzureBlobStore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	blobURL := z.containerURL.NewBlockBlobURL(blobPath(blobName))
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if _, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{}); err != nil {
		if z.isAzureError(err, azblob.ServiceCodeBlobNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
