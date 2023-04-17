package aws

import (
	"bytes"
	"context"
	"flag"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
)

var (
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
)

const (
	// Prometheus BlobstoreTypeLabel values
	awsS3Label        = "aws_s3"
	bucketWaitTimeout = 10 * time.Second
)

// AWS stuff
type AwsS3BlobStore struct {
	s3         *s3.S3
	bucket     *string
	downloader *s3manager.Downloader
	uploader   *s3manager.Uploader
}

func UseAwsS3BlobStore() bool {
	return *awsS3Bucket != ""
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
	util.RecordReadMetrics(awsS3Label, start, b, err)
	return util.Decompress(b, err)
}

func (a *AwsS3BlobStore) download(ctx context.Context, blobName string) ([]byte, error) {
	buff := &aws.WriteAtBuffer{}

	ctx, spn := tracing.StartSpan(ctx)
	_, err := a.downloader.DownloadWithContext(ctx, buff, &s3.GetObjectInput{
		Bucket: a.bucket,
		Key:    aws.String(util.BlobPath(blobName)),
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
	compressedData, err := util.Compress(data)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	n, err := a.upload(ctx, blobName, compressedData)
	util.RecordWriteMetrics(awsS3Label, start, n, err)
	return n, err
}

func (a *AwsS3BlobStore) upload(ctx context.Context, blobName string, compressedData []byte) (int, error) {
	uploadParams := &s3manager.UploadInput{
		Bucket: a.bucket,
		Key:    aws.String(util.BlobPath(blobName)),
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
	util.RecordDeleteMetrics(awsS3Label, start, err)
	return err
}

func (a *AwsS3BlobStore) delete(ctx context.Context, blobName string) error {
	deleteParams := &s3.DeleteObjectInput{
		Bucket: a.bucket,
		Key:    aws.String(util.BlobPath(blobName)),
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
		Key:    aws.String(util.BlobPath(blobName)),
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

func (a *AwsS3BlobStore) Writer(ctx context.Context, blobName string) (interfaces.CommittedWriteCloser, error) {
	// Open a pipe.
	pr, pw := io.Pipe()

	errch := make(chan error, 1)

	ctx, cancel := context.WithCancel(ctx)

	// Upload from pr in a separate Go routine.
	go func() {
		defer pr.Close()
		_, err := a.uploader.UploadWithContext(
			ctx,
			&s3manager.UploadInput{
				Bucket: a.bucket,
				Key:    aws.String(util.BlobPath(blobName)),
				Body:   pr,
			},
		)
		errch <- err
		close(errch)
	}()

	zw := util.NewCompressWriter(pw)
	cwc := ioutil.NewCustomCommitWriteCloser(zw)
	cwc.CommitFn = func(int64) error {
		if compresserCloseErr := zw.Close(); compresserCloseErr != nil {
			cancel() // Don't try to finish the commit op if Close() failed.
			if pipeCloseErr := pw.Close(); pipeCloseErr != nil {
				log.Errorf("Error closing the pipe for %s: %s", blobName, pipeCloseErr)
			}
			// Canceling the context makes any error in errch meaningless; don't
			// bother to read it.
			return compresserCloseErr
		}
		if writerCloseErr := pw.Close(); writerCloseErr != nil {
			return writerCloseErr
		}
		return <-errch
	}
	cwc.CloseFn = func() error {
		cancel()
		return nil
	}
	return cwc, nil
}
