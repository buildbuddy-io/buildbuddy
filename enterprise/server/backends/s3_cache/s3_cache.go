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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
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

	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var (
	region                   = flag.String("cache.s3.region", "", "The AWS region.")
	bucket                   = flag.String("cache.s3.bucket", "", "The AWS S3 bucket to store files in.")
	pathPrefix               = flag.String("cache.s3.path_prefix", "", "Prefix inside the AWS S3 bucket to store files")
	credentialsProfile       = flag.String("cache.s3.credentials_profile", "", "A custom credentials profile to use.")
	ttlDays                  = flag.Int("cache.s3.ttl_days", 0, "The period after which cache files should be TTLd. Disabled if 0.")
	webIdentityTokenFilePath = flag.String("cache.s3.web_identity_token_file", "", "The file path to the web identity token file.")
	roleARN                  = flag.String("cache.s3.role_arn", "", "The role ARN to use for web identity auth.")
	roleSessionName          = flag.String("cache.s3.role_session_name", "", "The role session name to use for web identity auth.")
	endpoint                 = flag.String("cache.s3.endpoint", "", "The AWS endpoint to use, useful for configuring the use of MinIO.")
	staticCredentialsID      = flag.String("cache.s3.static_credentials_id", "", "Static credentials ID to use, useful for configuring the use of MinIO.")
	staticCredentialsSecret  = flagutil.New("cache.s3.static_credentials_secret", "", "Static credentials secret to use, useful for configuring the use of MinIO.", flagutil.SecretTag)
	staticCredentialsToken   = flag.String("cache.s3.static_credentials_token", "", "Static credentials token to use, useful for configuring the use of MinIO.")
	disableSSL               = flagutil.New("cache.s3.disable_ssl", false, "Disables the use of SSL, useful for configuring the use of MinIO.", flagutil.DeprecatedTag("Specify a non-HTTPS endpoint instead."))
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
	client     *s3.Client
	bucket     *string
	pathPrefix string
	downloader *s3manager.Downloader
	uploader   *s3manager.Uploader
	ttlInDays  int32
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

	configOptions := []func(*config.LoadOptions) error{
		config.WithRegion(*region),
	}
	// See https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials
	if *credentialsProfile != "" {
		configOptions = append(configOptions, config.WithSharedConfigProfile(*credentialsProfile))
	}
	if *staticCredentialsID != "" && *staticCredentialsSecret != "" {
		configOptions = append(configOptions, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(*staticCredentialsID, *staticCredentialsSecret, *staticCredentialsToken),
		))
	}
	if *endpoint != "" {
		configOptions = append(configOptions, config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(
				func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
					return aws.Endpoint{
						URL:               *endpoint,
						SigningRegion:     *region,
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

	if *webIdentityTokenFilePath != "" {
		cfg.Credentials = stscreds.NewWebIdentityRoleProvider(
			sts.NewFromConfig(cfg),
			*roleARN,
			stscreds.IdentityTokenFile(*webIdentityTokenFilePath),
			func(o *stscreds.WebIdentityRoleOptions) {
				o.RoleSessionName = *roleSessionName
			},
		)
	}

	// Create S3 service client
	client := s3.NewFromConfig(
		cfg,
		func(o *s3.Options) { o.UsePathStyle = *forcePathStyle },
	)
	s3c := &S3Cache{
		client:     client,
		bucket:     bucket,
		pathPrefix: *pathPrefix,
		downloader: s3manager.NewDownloader(client),
		uploader:   s3manager.NewUploader(client),
		ttlInDays:  int32(*ttlDays),
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
			if err := s3c.setBucketTTL(ctx, *bucket, int32(*ttlDays)); err != nil {
				log.Printf("Error setting bucket TTL: %s", err)
			}
		}
	}
	return s3c, nil
}

func (s3c *S3Cache) bucketExists(ctx context.Context, bucketName string) (bool, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if _, err := s3c.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucketName}); err != nil {
		if isNotFoundErr(err) {
			return false, nil
		}
		return false, err
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
		if _, err := s3c.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucketName}); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, bucketWaitTimeout)
		defer cancel()
		return s3.NewBucketExistsWaiter(s3c.client).Wait(ctx, &s3.HeadBucketInput{Bucket: &bucketName}, bucketWaitTimeout)
	}
	return nil
}

func (s3c *S3Cache) setBucketTTL(ctx context.Context, bucketName string, ageInDays int32) error {
	traceCtx, spn := tracing.StartSpan(ctx)
	spn.SetName("GetBucketLifecycleConfigurationWithContext for S3Cache SetBucketTTL")
	attrs, err := s3c.client.GetBucketLifecycleConfiguration(traceCtx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: &bucketName,
	})
	spn.End()
	if err != nil {
		awsErr, ok := unwrap(err).(smithy.APIError)
		if ok && awsErr.ErrorCode() != "NoSuchLifecycleConfiguration" {
			return err
		}
	}
	if attrs != nil {
		for _, rule := range attrs.Rules {
			if rule.Expiration == nil {
				break
			}

			if rule.Status != s3types.ExpirationStatusEnabled {
				break
			}

			if rule.Expiration.Days == ageInDays {
				return nil
			}
		}
	}
	traceCtx, spn = tracing.StartSpan(ctx)
	spn.SetName("PutBucketLifecycleConfigurationWithContext for S3Cache SetBucketTTL")
	defer spn.End()
	_, err = s3c.client.PutBucketLifecycleConfiguration(traceCtx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: &bucketName,
		LifecycleConfiguration: &s3types.BucketLifecycleConfiguration{
			Rules: []s3types.LifecycleRule{
				{
					Expiration: &s3types.LifecycleExpiration{
						Days: ageInDays,
					},
					Status: s3types.ExpirationStatusEnabled,
					Filter: &s3types.LifecycleRuleFilterMemberPrefix{
						Value: "",
					},
				},
			},
		},
	})
	return err
}

func (s3c *S3Cache) key(ctx context.Context, r *rspb.ResourceName) (string, error) {
	rn := digest.ResourceNameFromProto(r)
	if err := rn.Validate(); err != nil {
		return "", err
	}
	hash := rn.GetDigest().GetHash()
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", err
	}
	isolationPrefix := filepath.Join(r.GetInstanceName(), digest.CacheTypeToPrefix(r.GetCacheType()))
	k := filepath.Join(s3c.pathPrefix, userPrefix, isolationPrefix, hash, hash)
	return k, nil
}

func unwrap(err error) error {
	for unwrappable, ok := err.(interface{ Unwrap() error }); ok; unwrappable, ok = unwrappable.Unwrap().(interface{ Unwrap() error }) {
		err = unwrappable.Unwrap()
	}
	return err
}

func isNotFoundErr(err error) bool {
	awsErr, ok := unwrap(err).(smithy.APIError)
	if !ok {
		return false
	}
	switch awsErr.ErrorCode() {
	case "NotFound", "NoSuchKey":
		return true
	default:
		return false
	}
}

func (s3c *S3Cache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	k, err := s3c.key(ctx, r)
	if err != nil {
		return nil, err
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	b, err := s3c.get(ctx, r.GetDigest(), k)
	timer.ObserveGet(len(b), err)
	return b, err
}

func (s3c *S3Cache) get(ctx context.Context, d *repb.Digest, key string) ([]byte, error) {
	buff := &s3manager.WriteAtBuffer{}
	ctx, spn := tracing.StartSpan(ctx)
	_, err := s3c.downloader.Download(ctx, buff, &s3.GetObjectInput{
		Bucket: s3c.bucket,
		Key:    &key,
	})
	spn.End()
	if isNotFoundErr(err) {
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}
	return buff.Bytes(), err
}

func (s3c *S3Cache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest][]byte, len(resources))
	eg, ctx := errgroup.WithContext(ctx)

	for _, r := range resources {
		fetchFn := func(r *rspb.ResourceName) {
			eg.Go(func() error {
				data, err := s3c.Get(ctx, r)
				if err != nil {
					return err
				}
				lock.Lock()
				defer lock.Unlock()
				foundMap[r.GetDigest()] = data
				return nil
			})
		}
		fetchFn(r)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return foundMap, nil
}

func (s3c *S3Cache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	k, err := s3c.key(ctx, r)
	if err != nil {
		return err
	}
	uploadParams := &s3.PutObjectInput{
		Bucket: s3c.bucket,
		Key:    &k,
		Body:   bytes.NewReader(data),
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	ctx, spn := tracing.StartSpan(ctx)
	_, err = s3c.uploader.Upload(ctx, uploadParams)
	spn.End()
	timer.ObserveSet(len(data), err)
	return err
}

func (s3c *S3Cache) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
	eg, ctx := errgroup.WithContext(ctx)

	for r, data := range kvs {
		setFn := func(r *rspb.ResourceName, data []byte) {
			eg.Go(func() error {
				return s3c.Set(ctx, r, data)
			})
		}
		setFn(r, data)
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (s3c *S3Cache) Delete(ctx context.Context, r *rspb.ResourceName) error {
	k, err := s3c.key(ctx, r)
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
		Key:    &key,
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if _, err := s3c.client.DeleteObject(ctx, deleteParams); err != nil {
		return err
	}

	return s3.NewObjectNotExistsWaiter(s3c.client).Wait(
		ctx,
		&s3.HeadObjectInput{
			Bucket: s3c.bucket,
			Key:    &key,
		},
		bucketWaitTimeout,
	)
}

func (s3c *S3Cache) bumpTTLIfStale(ctx context.Context, key string, t time.Time) bool {
	if s3c.ttlInDays == 0 || int32(time.Since(t).Hours()) < 24*s3c.ttlInDays/2 {
		return true
	}
	src := fmt.Sprintf("%s/%s", *s3c.bucket, key)
	input := &s3.CopyObjectInput{
		CopySource:        &src,
		Bucket:            s3c.bucket,
		Key:               &key,
		MetadataDirective: s3types.MetadataDirectiveReplace,
	}
	_, spn := tracing.StartSpan(ctx)
	_, err := s3c.client.CopyObject(ctx, input)
	spn.End()
	if isNotFoundErr(err) {
		return false
	}
	if err != nil {
		log.Printf("Error bumping TTL for key %s: %s", key, err.Error())
	}
	return true
}

func (s3c *S3Cache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	var err error
	defer timer.ObserveContains(err)

	metadata, err := s3c.metadata(ctx, r)
	if err != nil || metadata == nil {
		return false, err
	}

	// Bump TTL to ensure that referenced blobs are available and will be for some period of time afterwards,
	// as specified by the protocol description
	key, err := s3c.key(ctx, r)
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
func (s3c *S3Cache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	metadata, err := s3c.metadata(ctx, r)
	if err != nil {
		return nil, err
	}
	if metadata == nil {
		d := r.GetDigest()
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}

	// TODO - Add digest size support for AC
	digestSizeBytes := int64(-1)
	if r.GetCacheType() == rspb.CacheType_CAS {
		digestSizeBytes = metadata.ContentLength
	}

	return &interfaces.CacheMetadata{
		StoredSizeBytes:    metadata.ContentLength,
		DigestSizeBytes:    digestSizeBytes,
		LastModifyTimeUsec: metadata.LastModified.UnixMicro(),
	}, nil
}

func (s3c *S3Cache) metadata(ctx context.Context, r *rspb.ResourceName) (*s3.HeadObjectOutput, error) {
	key, err := s3c.key(ctx, r)
	if err != nil {
		return nil, err
	}
	params := &s3.HeadObjectInput{
		Bucket: s3c.bucket,
		Key:    &key,
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	head, err := s3c.client.HeadObject(ctx, params)
	if err != nil {
		if isNotFoundErr(err) {
			return nil, nil
		}
		return nil, err
	}

	return head, nil
}

func (s3c *S3Cache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	lock := sync.RWMutex{} // protects(missing)
	var missing []*repb.Digest
	eg, ctx := errgroup.WithContext(ctx)

	for _, r := range resources {
		fetchFn := func(r *rspb.ResourceName) {
			eg.Go(func() error {
				exists, err := s3c.Contains(ctx, r)
				if err != nil {
					return err
				}
				if !exists {
					lock.Lock()
					defer lock.Unlock()
					missing = append(missing, r.GetDigest())
				}
				return nil
			})
		}
		fetchFn(r)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return missing, nil
}

func (s3c *S3Cache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	k, err := s3c.key(ctx, r)
	if err != nil {
		return nil, err
	}
	ctx, spn := tracing.StartSpan(ctx)
	// TODO(bduffany): track this as a contains() request, or find a way to
	// track it as part of the read

	// This range follows the format specified here: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	readRange := fmt.Sprintf("bytes=%d-", uncompressedOffset)
	if limit != 0 {
		// range bounds are inclusive
		readRange = fmt.Sprintf("bytes=%d-%d", uncompressedOffset, uncompressedOffset+limit-1)
	}

	result, err := s3c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: s3c.bucket,
		Key:    &k,
		Range:  &readRange,
	})
	spn.End()
	if isNotFoundErr(err) {
		d := r.GetDigest()
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	} else if err != nil {
		return nil, status.InternalErrorf("Error getting s3 object at key %s for cache: %v", k, err)
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	return io.NopCloser(timer.NewInstrumentedReader(result.Body, r.GetDigest().GetSizeBytes())), err
}

type waitForUploadWriteCloser struct {
	io.WriteCloser
	ctx           context.Context
	cancelFunc    context.CancelFunc
	finishedWrite chan struct{}
	timer         *cache_metrics.CacheTimer
	size          int64
}

func (w *waitForUploadWriteCloser) Write(p []byte) (int, error) {
	_, spn := tracing.StartSpan(w.ctx)
	defer spn.End()
	return w.WriteCloser.Write(p)
}

func (w *waitForUploadWriteCloser) Commit() error {
	err := w.WriteCloser.Close()
	if err != nil {
		return err
	}
	<-w.finishedWrite
	return nil
}

func (w *waitForUploadWriteCloser) Close() error {
	w.cancelFunc()
	return nil
}

func (s3c *S3Cache) Writer(ctx context.Context, rn *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	k, err := s3c.key(ctx, rn)
	if err != nil {
		return nil, err
	}
	// TODO(tempoz): r is only closed in case of error
	r, w := io.Pipe()
	uploadParams := &s3.PutObjectInput{
		Bucket: s3c.bucket,
		Key:    &k,
		Body:   r,
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	ctx, cancel := context.WithCancel(ctx)
	closer := &waitForUploadWriteCloser{
		WriteCloser:   w,
		ctx:           ctx,
		cancelFunc:    cancel,
		finishedWrite: make(chan struct{}),
		timer:         timer,
		size:          rn.GetDigest().GetSizeBytes(),
	}
	go func() {
		if _, err = s3c.uploader.Upload(ctx, uploadParams); err != nil {
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

func (s3c *S3Cache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	return compressor == repb.Compressor_IDENTITY
}

func (s3c *S3Cache) SupportsEncryption(ctx context.Context) bool {
	return false
}
