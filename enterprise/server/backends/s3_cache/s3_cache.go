package s3_cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/cache_metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
	ttlInDays  int64
	prefix     string
}

func NewS3Cache(awsConfig *config.S3CacheConfig) (*S3Cache, error) {
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
	s3c := &S3Cache{
		s3:         svc,
		bucket:     aws.String(awsConfig.Bucket),
		downloader: s3manager.NewDownloader(sess),
		uploader:   s3manager.NewUploader(sess),
		ttlInDays:  awsConfig.TTLDays,
	}

	// S3 access points can't modify or delete buckets
	// https://github.com/awsdocs/amazon-s3-developer-guide/blob/master/doc_source/access-points.md
	const s3AccessPointPrefix = "arn:aws:s3"
	if strings.HasPrefix(awsConfig.Bucket, s3AccessPointPrefix) {
		log.Printf("Encountered an S3 access point %s...not creating bucket", awsConfig.Bucket)
	} else {
		if err := s3c.createBucketIfNotExists(ctx, awsConfig.Bucket); err != nil {
			return nil, err
		}
		if awsConfig.TTLDays > 0 {
			if err := s3c.setBucketTTL(ctx, awsConfig.Bucket, awsConfig.TTLDays); err != nil {
				log.Printf("Error setting bucket TTL: %s", err)
			}
		}
	}
	return s3c, nil
}

func (s3c *S3Cache) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	// HeadBucket call will return 404 or 403
	if _, err := s3c.s3.HeadBucketWithContext(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)}); err != nil {
		awsErr := err.(awserr.Error)
		// AWS returns codes as strings
		// https://github.com/aws/aws-sdk-go/blob/master/service/s3/s3manager/bucket_region_test.go#L70
		if awsErr.Code() != "NotFound" {
			return err
		}

		log.Printf("Creating storage bucket: %s", bucketName)
		if _, err := s3c.s3.CreateBucketWithContext(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)}); err != nil {
			return err
		}
		return s3c.s3.WaitUntilBucketExistsWithContext(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
	}
	return nil
}

func (s3c *S3Cache) setBucketTTL(ctx context.Context, bucketName string, ageInDays int64) error {
	attrs, err := s3c.s3.GetBucketLifecycleConfigurationWithContext(ctx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucketName),
	})
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
	_, err = s3c.s3.PutBucketLifecycleConfigurationWithContext(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucketName),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				&s3.LifecycleRule{
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

func (s3c *S3Cache) WithPrefix(prefix string) interfaces.Cache {
	newPrefix := filepath.Join(append(filepath.SplitList(s3c.prefix), prefix)...)
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
	}
}

func (s3c *S3Cache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	k, err := s3c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	b, err := s3c.get(ctx, d, k)
	timer.EndGet(len(b), err)
	return b, err
}

func (s3c *S3Cache) get(ctx context.Context, d *repb.Digest, key string) ([]byte, error) {
	buff := &aws.WriteAtBuffer{}
	_, err := s3c.downloader.DownloadWithContext(ctx, buff, &s3.GetObjectInput{
		Bucket: s3c.bucket,
		Key:    aws.String(key),
	})
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
	_, err = s3c.uploader.UploadWithContext(ctx, uploadParams)
	timer.EndSet(len(data), err)
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
	timer.EndDelete(err)
	return err
}

func (s3c *S3Cache) delete(ctx context.Context, key string) error {
	deleteParams := &s3.DeleteObjectInput{
		Bucket: s3c.bucket,
		Key:    aws.String(key),
	}

	if _, err := s3c.s3.DeleteObjectWithContext(ctx, deleteParams); err != nil {
		return err
	}

	return s3c.s3.WaitUntilObjectNotExistsWithContext(ctx, &s3.HeadObjectInput{
		Bucket: s3c.bucket,
		Key:    aws.String(key),
	})
}

func (s3c *S3Cache) bumpTTLIfStale(ctx context.Context, key string, t time.Time) bool {
	if int64(time.Since(t).Hours()) < 24*s3c.ttlInDays/2 {
		return true
	}
	input := &s3.CopyObjectInput{
		CopySource:        aws.String(fmt.Sprintf("%s/%s", *s3c.bucket, key)),
		Bucket:            s3c.bucket,
		Key:               aws.String(key),
		MetadataDirective: aws.String(s3.MetadataDirectiveReplace),
	}
	_, err := s3c.s3.CopyObject(input)
	if isNotFoundErr(err) {
		return false
	}
	if err != nil {
		log.Printf("Error bumping TTL for key %s: %s", key, err.Error())
	}
	return true
}

func (s3c *S3Cache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	k, err := s3c.key(ctx, d)
	if err != nil {
		return false, err
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	c, err := s3c.contains(ctx, k)
	timer.EndContains(err)
	return c, err
}

func (s3c *S3Cache) contains(ctx context.Context, key string) (bool, error) {
	params := &s3.HeadObjectInput{
		Bucket: s3c.bucket,
		Key:    aws.String(key),
	}

	head, err := s3c.s3.HeadObjectWithContext(ctx, params)
	if err != nil {
		if isNotFoundErr(err) {
			return false, nil
		}
		return false, err
	}
	return s3c.bumpTTLIfStale(ctx, key, *head.LastModified), nil
}

func (s3c *S3Cache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest]bool, len(digests))
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				exists, err := s3c.Contains(ctx, d)
				if err != nil {
					return err
				}
				lock.Lock()
				defer lock.Unlock()
				foundMap[d] = exists
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

func (s3c *S3Cache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.Reader, error) {
	k, err := s3c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	// TODO(bduffany): track this as a contains() request, or find a way to
	// track it as part of the read
	result, err := s3c.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: s3c.bucket,
		Key:    aws.String(k),
		Range:  aws.String(fmt.Sprintf("%d-", offset)),
	})
	if isNotFoundErr(err) {
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	return timer.NewInstrumentedReader(result.Body, d.GetSizeBytes()), err
}

type waitForUploadWriteCloser struct {
	io.WriteCloser
	finishedWrite chan struct{}
	timer         *cache_metrics.CacheTimer
	size          int64
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
	r, w := io.Pipe()
	uploadParams := &s3manager.UploadInput{
		Bucket: s3c.bucket,
		Key:    aws.String(k),
		Body:   r,
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	closer := &waitForUploadWriteCloser{
		WriteCloser:   w,
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
