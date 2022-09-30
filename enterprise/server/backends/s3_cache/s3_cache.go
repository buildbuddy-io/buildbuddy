package s3_cache

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/cache_metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	region                   = flag.String("cache.s3.region", "", "The AWS region.")
	bucket                   = flag.String("cache.s3.bucket", "", "The AWS S3 bucket to store files in.")
	credentialsProfile       = flag.String("cache.s3.credentials_profile", "", "A custom credentials profile to use.")
	ttlDays                  = flag.Int64("cache.s3.ttl_days", 0, "The period after which cache files should be TTLd. Disabled if 0.")
	webIdentityTokenFilePath = flag.String("cache.s3.web_identity_token_file", "", "The file path to the web identity token file.")
	roleARN                  = flag.String("cache.s3.role_arn", "", "The role ARN to use for web identity auth.")
	roleSessionName          = flag.String("cache.s3.role_session_name", "", "The role session name to use for web identity auth.")
	endpoint                 = flag.String("cache.s3.endpoint", "", "The AWS endpoint to use, useful for configuring the use of MinIO.")
	staticCredentialsID      = flag.String("cache.s3.static_credentials_id", "", "Static credentials ID to use, useful for configuring the use of MinIO.")
	staticCredentialsSecret  = flagutil.New("cache.s3.static_credentials_secret", "", "Static credentials secret to use, useful for configuring the use of MinIO.", flagutil.SecretTag)
	staticCredentialsToken   = flag.String("cache.s3.static_credentials_token", "", "Static credentials token to use, useful for configuring the use of MinIO.")
	disableSSL               = flag.Bool("cache.s3.disable_ssl", false, "Disables the use of SSL, useful for configuring the use of MinIO.")
	forcePathStyle           = flag.Bool("cache.s3.s3_force_path_style", false, "Force path style urls for objects, useful for configuring the use of MinIO.")
)

const (
	bucketWaitTimeout = 10 * time.Second
)

var (
	cacheLabels = cache_metrics.MakeCacheLabels(cache_metrics.CloudCacheTier, "aws_s3")
)

// AWS stuff
type S3Cache struct {
	s3         *s3.S3
	bucket     *string
	downloader *s3manager.Downloader
	uploader   *s3manager.Uploader
	prefix     string
	ttlInDays  int64
}

func Register(env environment.Env) error {
	if *bucket == "" {
		return nil
	}
	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with s3_cache.")
	}
	s3Cache, err := NewS3Cache()
	if err != nil {
		return status.InternalErrorf("Error configuring S3 cache: %s", err)
	}
	env.SetCache(s3Cache)
	return nil
}

func NewS3Cache() (*S3Cache, error) {
	ctx := context.Background()

	config := &aws.Config{
		Region: aws.String(*region),
	}
	// See https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials
	if *credentialsProfile != "" {
		config.Credentials = credentials.NewSharedCredentials("", *credentialsProfile)
	}
	if *staticCredentialsID != "" && *staticCredentialsSecret != "" {
		config.Credentials = credentials.NewStaticCredentials(*staticCredentialsID, *staticCredentialsSecret, *staticCredentialsToken)
	}
	if *endpoint != "" {
		config.Endpoint = aws.String(*endpoint)
	}
	if *disableSSL {
		config.DisableSSL = aws.Bool(*disableSSL)
	}
	if *forcePathStyle {
		config.S3ForcePathStyle = aws.Bool(*forcePathStyle)
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}

	if *webIdentityTokenFilePath != "" {
		config.Credentials = credentials.NewCredentials(stscreds.NewWebIdentityRoleProvider(sts.New(sess), *roleARN, *roleSessionName, *webIdentityTokenFilePath))
	}

	// Create S3 service client
	svc := s3.New(sess)
	s3c := &S3Cache{
		s3:         svc,
		bucket:     aws.String(*bucket),
		downloader: s3manager.NewDownloader(sess),
		uploader:   s3manager.NewUploader(sess),
		ttlInDays:  *ttlDays,
	}

	// S3 access points can't modify or delete buckets
	// https://github.com/awsdocs/amazon-s3-developer-guide/blob/master/doc_source/access-points.md
	const s3AccessPointPrefix = "arn:aws:s3"
	if strings.HasPrefix(*bucket, s3AccessPointPrefix) {
		log.Printf("Encountered an S3 access point %s...not creating bucket", *bucket)
	} else {
		if err := s3c.createBucketIfNotExists(ctx, *bucket); err != nil {
			return nil, err
		}
		if *ttlDays > 0 {
			if err := s3c.setBucketTTL(ctx, *bucket, *ttlDays); err != nil {
				log.Printf("Error setting bucket TTL: %s", err)
			}
		}
	}
	return s3c, nil
}

func (s3c *S3Cache) bucketExists(ctx context.Context, bucketName string) (bool, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if _, err := s3c.s3.HeadBucketWithContext(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)}); err != nil {
		aerr := err.(awserr.Error)
		// AWS returns codes as strings
		// https://github.com/aws/aws-sdk-go/blob/master/service/s3/s3manager/bucket_region_test.go#L70
		if aerr.Code() != "NotFound" {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

func (s3c *S3Cache) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	// HeadBucket call will return 404 or 403
	if exists, err := s3c.bucketExists(ctx, bucketName); err != nil {
		return err
	} else if !exists {
		log.Printf("Creating storage bucket: %s", bucketName)
		ctx, spn := tracing.StartSpan(ctx)
		defer spn.End()
		if _, err := s3c.s3.CreateBucketWithContext(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)}); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, bucketWaitTimeout)
		defer cancel()
		return s3c.s3.WaitUntilBucketExistsWithContext(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
	}
	return nil
}

func (s3c *S3Cache) setBucketTTL(ctx context.Context, bucketName string, ageInDays int64) error {
	traceCtx, spn := tracing.StartSpan(ctx)
	spn.SetName("GetBucketLifecycleConfigurationWithContext for S3Cache SetBucketTTL")
	attrs, err := s3c.s3.GetBucketLifecycleConfigurationWithContext(traceCtx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucketName),
	})
	spn.End()
	if err != nil {
		awsErr, ok := err.(awserr.Error)
		if ok && awsErr.Code() != "NoSuchLifecycleConfiguration" {
			return err
		}
	}
	for _, rule := range attrs.Rules {
		if rule == nil || rule.Expiration == nil {
			break
		}

		if rule.Status == nil || *(rule.Status) != "Enabled" {
			break
		}

		if rule.Expiration.Days != nil && *(rule.Expiration.Days) == ageInDays {
			return nil
		}
	}
	traceCtx, spn = tracing.StartSpan(ctx)
	spn.SetName("PutBucketLifecycleConfigurationWithContext for S3Cache SetBucketTTL")
	defer spn.End()
	_, err = s3c.s3.PutBucketLifecycleConfigurationWithContext(traceCtx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucketName),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(ageInDays),
					},
					Status: aws.String("Enabled"),
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
				},
			},
		},
	})
	return err
}

func (s3c *S3Cache) key(ctx context.Context, d *repb.Digest) (string, error) {
	hash, err := digest.Validate(d)
	if err != nil {
		return "", err
	}
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", err
	}
	k := filepath.Join(userPrefix, s3c.prefix, hash, hash)
	return k, nil
}

func isNotFoundErr(err error) bool {
	awsErr, ok := err.(awserr.Error)
	if !ok {
		return false
	}
	switch awsErr.Code() {
	case "NotFound", "NoSuchKey":
		return true
	default:
		return false
	}
}

func (s3c *S3Cache) WithIsolation(ctx context.Context, cacheType interfaces.CacheTypeDeprecated, remoteInstanceName string) (interfaces.Cache, error) {
	newPrefix := filepath.Join(remoteInstanceName, cacheType.Prefix())
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}
	return &S3Cache{
		s3:         s3c.s3,
		bucket:     s3c.bucket,
		downloader: s3c.downloader,
		uploader:   s3c.uploader,
		ttlInDays:  s3c.ttlInDays,
		prefix:     newPrefix,
	}, nil
}

func (s3c *S3Cache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	k, err := s3c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	b, err := s3c.get(ctx, d, k)
	timer.ObserveGet(len(b), err)
	return b, err
}

func (s3c *S3Cache) get(ctx context.Context, d *repb.Digest, key string) ([]byte, error) {
	buff := &aws.WriteAtBuffer{}
	ctx, spn := tracing.StartSpan(ctx)
	_, err := s3c.downloader.DownloadWithContext(ctx, buff, &s3.GetObjectInput{
		Bucket: s3c.bucket,
		Key:    aws.String(key),
	})
	spn.End()
	if isNotFoundErr(err) {
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}
	return buff.Bytes(), err
}

func (s3c *S3Cache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest][]byte, len(digests))
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				data, err := s3c.Get(ctx, d)
				if err != nil {
					return err
				}
				lock.Lock()
				defer lock.Unlock()
				foundMap[d] = data
				return nil
			})
		}
		fetchFn(d)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return foundMap, nil
}
func (s3c *S3Cache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	k, err := s3c.key(ctx, d)
	if err != nil {
		return err
	}
	uploadParams := &s3manager.UploadInput{
		Bucket: s3c.bucket,
		Key:    aws.String(k),
		Body:   bytes.NewReader(data),
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	ctx, spn := tracing.StartSpan(ctx)
	_, err = s3c.uploader.UploadWithContext(ctx, uploadParams)
	spn.End()
	timer.ObserveSet(len(data), err)
	return err
}

func (s3c *S3Cache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	eg, ctx := errgroup.WithContext(ctx)

	for d, data := range kvs {
		setFn := func(d *repb.Digest, data []byte) {
			eg.Go(func() error {
				return s3c.Set(ctx, d, data)
			})
		}
		setFn(d, data)
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (s3c *S3Cache) Delete(ctx context.Context, d *repb.Digest) error {
	k, err := s3c.key(ctx, d)
	if err != nil {
		return err
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	err = s3c.delete(ctx, k)
	timer.ObserveDelete(err)
	return err
}

func (s3c *S3Cache) delete(ctx context.Context, key string) error {
	deleteParams := &s3.DeleteObjectInput{
		Bucket: s3c.bucket,
		Key:    aws.String(key),
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if _, err := s3c.s3.DeleteObjectWithContext(ctx, deleteParams); err != nil {
		return err
	}

	return s3c.s3.WaitUntilObjectNotExistsWithContext(ctx, &s3.HeadObjectInput{
		Bucket: s3c.bucket,
		Key:    aws.String(key),
	})
}

func (s3c *S3Cache) bumpTTLIfStale(ctx context.Context, key string, t time.Time) bool {
	if s3c.ttlInDays == 0 || int64(time.Since(t).Hours()) < 24*s3c.ttlInDays/2 {
		return true
	}
	input := &s3.CopyObjectInput{
		CopySource:        aws.String(fmt.Sprintf("%s/%s", *s3c.bucket, key)),
		Bucket:            s3c.bucket,
		Key:               aws.String(key),
		MetadataDirective: aws.String(s3.MetadataDirectiveReplace),
	}
	_, spn := tracing.StartSpan(ctx)
	_, err := s3c.s3.CopyObject(input)
	spn.End()
	if isNotFoundErr(err) {
		return false
	}
	if err != nil {
		log.Printf("Error bumping TTL for key %s: %s", key, err.Error())
	}
	return true
}

func (s3c *S3Cache) ContainsDeprecated(ctx context.Context, d *repb.Digest) (bool, error) {
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	var err error
	defer timer.ObserveContains(err)

	metadata, err := s3c.metadata(ctx, d)
	if err != nil || metadata == nil {
		return false, err
	}

	// Bump TTL to ensure that referenced blobs are available and will be for some period of time afterwards,
	// as specified by the protocol description
	key, err := s3c.key(ctx, d)
	if err != nil {
		return false, err
	}
	bumped := s3c.bumpTTLIfStale(ctx, key, *metadata.LastModified)
	if bumped {
		return true, nil
	}
	return false, err
}

// TODO(buildbuddy-internal#1485) - Add last access time
func (s3c *S3Cache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	metadata, err := s3c.metadata(ctx, d)
	if err != nil {
		return nil, err
	}
	if metadata == nil {
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}
	return &interfaces.CacheMetadata{
		SizeBytes:          *metadata.ContentLength,
		LastModifyTimeUsec: metadata.LastModified.UnixMicro(),
	}, nil
}

func (s3c *S3Cache) metadata(ctx context.Context, d *repb.Digest) (*s3.HeadObjectOutput, error) {
	key, err := s3c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	params := &s3.HeadObjectInput{
		Bucket: s3c.bucket,
		Key:    aws.String(key),
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	head, err := s3c.s3.HeadObjectWithContext(ctx, params)
	if err != nil {
		if isNotFoundErr(err) {
			return nil, nil
		}
		return nil, err
	}

	return head, nil
}

func (s3c *S3Cache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	lock := sync.RWMutex{} // protects(missing)
	var missing []*repb.Digest
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				exists, err := s3c.ContainsDeprecated(ctx, d)
				if err != nil {
					return err
				}
				if !exists {
					lock.Lock()
					defer lock.Unlock()
					missing = append(missing, d)
				}
				return nil
			})
		}
		fetchFn(d)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return missing, nil
}

func (s3c *S3Cache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	k, err := s3c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	ctx, spn := tracing.StartSpan(ctx)
	// TODO(bduffany): track this as a contains() request, or find a way to
	// track it as part of the read

	// This range follows the format specified here: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	readRange := aws.String(fmt.Sprintf("bytes=%d-", offset))
	if limit != 0 {
		// range bounds are inclusive
		readRange = aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+limit-1))
	}

	result, err := s3c.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: s3c.bucket,
		Key:    aws.String(k),
		Range:  readRange,
	})
	spn.End()
	if isNotFoundErr(err) {
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	return io.NopCloser(timer.NewInstrumentedReader(result.Body, d.GetSizeBytes())), err
}

type waitForUploadWriteCloser struct {
	io.WriteCloser
	ctx           context.Context
	finishedWrite chan struct{}
	timer         *cache_metrics.CacheTimer
	size          int64
}

func (w *waitForUploadWriteCloser) Write(p []byte) (int, error) {
	_, spn := tracing.StartSpan(w.ctx)
	defer spn.End()
	return w.WriteCloser.Write(p)
}

func (w *waitForUploadWriteCloser) Close() error {
	err := w.WriteCloser.Close()
	if err != nil {
		return err
	}
	<-w.finishedWrite
	return nil
}

func (s3c *S3Cache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	k, err := s3c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	// TODO(tempoz): r is only closed in case of error
	r, w := io.Pipe()
	uploadParams := &s3manager.UploadInput{
		Bucket: s3c.bucket,
		Key:    aws.String(k),
		Body:   r,
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	closer := &waitForUploadWriteCloser{
		WriteCloser:   w,
		ctx:           ctx,
		finishedWrite: make(chan struct{}),
		timer:         timer,
		size:          d.GetSizeBytes(),
	}
	go func() {
		if _, err = s3c.uploader.UploadWithContext(ctx, uploadParams); err != nil {
			r.CloseWithError(err)
		}
		close(closer.finishedWrite)
	}()
	return closer, nil
}

func (s3c *S3Cache) Start() error {
	return nil
}

func (s3c *S3Cache) Stop() error {
	return nil
}
