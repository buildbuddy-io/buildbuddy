package aws

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"

	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	awsS3StaticCredentialsSecret  = flag.String("storage.aws_s3.static_credentials_secret", "", "Static credentials secret to use, useful for configuring the use of MinIO.", flag.Secret)
	awsS3StaticCredentialsToken   = flag.String("storage.aws_s3.static_credentials_token", "", "Static credentials token to use, useful for configuring the use of MinIO.")
	awsS3DisableSSL               = flag.Bool("storage.aws_s3.disable_ssl", false, "Disables the use of SSL, useful for configuring the use of MinIO.", flag.Deprecated("Specify a non-HTTPS endpoint instead."))
	awsS3ForcePathStyle           = flag.Bool("storage.aws_s3.s3_force_path_style", false, "Force path style urls for objects, useful for configuring the use of MinIO.")
)

const (
	// Prometheus BlobstoreTypeLabel values
	awsS3Label        = "aws_s3"
	bucketWaitTimeout = 10 * time.Second
)

// AWS stuff
type AwsS3BlobStore struct {
	client     *s3.Client
	bucket     *string
	downloader *s3manager.Downloader
	uploader   *s3manager.Uploader
}

func UseAwsS3BlobStore() bool {
	return *awsS3Bucket != ""
}

func NewAwsS3BlobStore(ctx context.Context) (*AwsS3BlobStore, error) {
	configOptions := []func(*config.LoadOptions) error{
		config.WithRegion(*awsS3Region),
	}
	// See https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials
	if *awsS3CredentialsProfile != "" {
		log.Debugf("AWS blobstore credentials profile found: %q", *awsS3CredentialsProfile)
		configOptions = append(configOptions, config.WithSharedConfigProfile(*awsS3CredentialsProfile))
	}
	if *awsS3StaticCredentialsID != "" && *awsS3StaticCredentialsSecret != "" {
		log.Debugf("AWS blobstore static credentials found: %q", *awsS3StaticCredentialsID)
		configOptions = append(configOptions, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(*awsS3StaticCredentialsID, *awsS3StaticCredentialsSecret, *awsS3StaticCredentialsToken),
		))
	}
	if *awsS3Endpoint != "" {
		log.Debugf("AWS blobstore endpoint found: %q", *awsS3Endpoint)
		configOptions = append(configOptions, config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(
				func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
					return aws.Endpoint{
						URL:               *awsS3Endpoint,
						SigningRegion:     *awsS3Region,
						HostnameImmutable: true,
					}, nil
				},
			),
		))
	}
	cfg, err := config.LoadDefaultConfig(ctx, configOptions...)
	if err != nil {
		return nil, err
	}
	log.Debug("AWS blobstore config created")

	if *awsS3WebIdentityTokenFilePath != "" {
		cfg.Credentials = stscreds.NewWebIdentityRoleProvider(
			sts.NewFromConfig(cfg),
			*awsS3RoleARN,
			stscreds.IdentityTokenFile(*awsS3WebIdentityTokenFilePath),
			func(o *stscreds.WebIdentityRoleOptions) {
				o.RoleSessionName = *awsS3RoleSessionName
			},
		)
		log.Debugf("AWS web identity credentials set (%s, %s, %s)", *awsS3RoleARN, *awsS3RoleSessionName, *awsS3WebIdentityTokenFilePath)
	}

	// Create S3 service client
	client := s3.NewFromConfig(
		cfg,
		func(o *s3.Options) {
			log.Debug("AWS blobstore forcing path style")
			o.UsePathStyle = *awsS3ForcePathStyle
		},
	)
	log.Debug("AWS blobstore service client created")

	awsBlobStore := &AwsS3BlobStore{
		client:     client,
		bucket:     awsS3Bucket,
		downloader: s3manager.NewDownloader(client),
		uploader:   s3manager.NewUploader(client),
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
	_, err := a.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucketName})
	spn.End()
	if err == nil {
		return true, nil
	}
	var nf *s3types.NotFound
	if errors.As(err, &nf) {
		return false, nil
	}
	return false, err
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
		if _, err := a.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucketName}); err != nil {
			return err
		}
		log.Debug("Waiting until AWS Bucket exists")
		ctx, cancel := context.WithTimeout(ctx, bucketWaitTimeout)
		defer cancel()
		return s3.NewBucketExistsWaiter(a.client).Wait(ctx, &s3.HeadBucketInput{Bucket: &bucketName}, bucketWaitTimeout)
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
	buff := &s3manager.WriteAtBuffer{}

	ctx, spn := tracing.StartSpan(ctx)
	_, err := a.downloader.Download(ctx, buff, &s3.GetObjectInput{
		Bucket: a.bucket,
		Key:    &blobName,
	})
	spn.End()

	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, status.NotFoundError(err.Error())
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
	uploadParams := &s3.PutObjectInput{
		Bucket: a.bucket,
		Key:    &blobName,
		Body:   bytes.NewReader(compressedData),
	}
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if _, err := a.uploader.Upload(ctx, uploadParams); err != nil {
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
		Key:    &blobName,
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if _, err := a.client.DeleteObject(ctx, deleteParams); err != nil {
		return err
	}

	return s3.NewObjectNotExistsWaiter(a.client).Wait(
		ctx,
		&s3.HeadObjectInput{
			Bucket: a.bucket,
			Key:    &blobName,
		},
		bucketWaitTimeout,
	)
}

func (a *AwsS3BlobStore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	params := &s3.HeadObjectInput{
		Bucket: a.bucket,
		Key:    &blobName,
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	_, err := a.client.HeadObject(ctx, params)
	if err != nil {
		var nf *s3types.NotFound
		if errors.As(err, &nf) {
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
		_, err := a.uploader.Upload(
			ctx,
			&s3.PutObjectInput{
				Bucket: a.bucket,
				Key:    &blobName,
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
		return pw.Close()
	}
	return cwc, nil
}
