package blobstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/api/option"
)

// Returns whatever blobstore is specified in the config.
func GetConfiguredBlobstore(c *config.Configurator) (interfaces.Blobstore, error) {
	if c.GetStorageDiskRootDir() != "" {
		return NewDiskBlobStore(c.GetStorageDiskRootDir()), nil
	}
	if gcsConfig := c.GetStorageGCSConfig(); gcsConfig != nil && gcsConfig.Bucket != "" {
		opts := make([]option.ClientOption, 0)
		if gcsConfig.CredentialsFile != "" {
			opts = append(opts, option.WithCredentialsFile(gcsConfig.CredentialsFile))
		}
		return NewGCSBlobStore(gcsConfig.Bucket, gcsConfig.ProjectID, opts...)
	}

	if awsConfig := c.GetStorageAWSS3Config(); awsConfig != nil && awsConfig.Bucket != "" {
		return NewAwsS3BlobStore(awsConfig)
	}
	return nil, fmt.Errorf("No storage backend configured -- please specify at least one in the config")
}

// A Disk-based blob storage implementation that reads and writes blobs to/from
// files.
type DiskBlobStore struct {
	rootDir string
}

func NewDiskBlobStore(rootDir string) *DiskBlobStore {
	return &DiskBlobStore{
		rootDir: rootDir,
	}
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
		log.Printf("zr err: %s", err)
		return nil, err
	}
	rsp, err := ioutil.ReadAll(zr)
	if err != nil {
		log.Printf("readall err: %s", err)
		return nil, err
	}
	if err := zr.Close(); err != nil {
		log.Printf("close err: %s", err)
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
	return disk.WriteFile(ctx, fullPath, compressedData)
}

func (d *DiskBlobStore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return nil, err
	}
	return decompress(disk.ReadFile(ctx, fullPath))
}

func (d *DiskBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return err
	}
	return disk.DeleteFile(ctx, fullPath)
}

func (d *DiskBlobStore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	fullPath, err := d.blobPath(blobName)
	if err != nil {
		return false, err
	}
	return disk.FileExists(ctx, fullPath)
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
	return g, nil
}

func (g *GCSBlobStore) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	if _, err := g.gcsClient.Bucket(bucketName).Attrs(ctx); err != nil {
		log.Printf("Creating storage bucket: %s", bucketName)
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
	return decompress(ioutil.ReadAll(reader))
}

func (g *GCSBlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	writer := g.bucketHandle.Object(blobName).NewWriter(ctx)
	defer writer.Close()
	compressedData, err := compress(data)
	if err != nil {
		return 0, err
	}
	return writer.Write(compressedData)
}

func (g *GCSBlobStore) DeleteBlob(ctx context.Context, blobName string) error {
	return g.bucketHandle.Object(blobName).Delete(ctx)
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

	var creds *credentials.Credentials

	// See https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials
	if awsConfig.CredentialsProfile != "" {
		creds = credentials.NewSharedCredentials("", awsConfig.CredentialsProfile)
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsConfig.Region),
		Credentials: creds,
	})

	if err != nil {
		return nil, err
	}

	// Create S3 service client
	svc := s3.New(sess)

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
		log.Printf("Encountered an S3 access point %s...not creating bucket", awsConfig.Bucket)
	} else {
		if err := awsBlobStore.createBucketIfNotExists(ctx, awsConfig.Bucket); err != nil {
			return nil, err
		}
	}

	return awsBlobStore, nil
}

func (a *AwsS3BlobStore) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	// HeadBucket call will return 404 or 403
	if _, err := a.s3.HeadBucketWithContext(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)}); err != nil {
		aerr := err.(awserr.Error)
		// AWS returns codes as strings
		// https://github.com/aws/aws-sdk-go/blob/master/service/s3/s3manager/bucket_region_test.go#L70
		if aerr.Code() != "NotFound" {
			return err
		}

		log.Printf("Creating storage bucket: %s", bucketName)
		if _, err := a.s3.CreateBucketWithContext(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)}); err != nil {
			return err
		}
		return a.s3.WaitUntilBucketExistsWithContext(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
	}
	return nil
}

func (a *AwsS3BlobStore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
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

	return decompress(buff.Bytes(), err)
}

func (a *AwsS3BlobStore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	compressedData, err := compress(data)
	if err != nil {
		return 0, err
	}
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
