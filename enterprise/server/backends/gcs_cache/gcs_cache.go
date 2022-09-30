package gcs_cache

import (
	"context"
	"errors"
	"flag"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/cache_metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	bucket          = flag.String("cache.gcs.bucket", "", "The name of the GCS bucket to store cache files in.")
	credentialsFile = flag.String("cache.gcs.credentials_file", "", "A path to a JSON credentials file that will be used to authenticate to GCS.")
	projectID       = flag.String("cache.gcs.project_id", "", "The Google Cloud project ID of the project owning the above credentials and GCS bucket.")
	ttlDays         = flag.Int64("cache.gcs.ttl_days", 0, "The period after which cache files should be TTLd. Disabled if 0.")
)

const (
	maxNumRetries = 3
)

var (
	cacheLabels = cache_metrics.MakeCacheLabels(cache_metrics.CloudCacheTier, "gcs")
)

type GCSCache struct {
	gcsClient    *storage.Client
	bucketHandle *storage.BucketHandle
	projectID    string
	prefix       string
	ttlInDays    int64
}

func Register(env environment.Env) error {
	if *bucket == "" {
		return nil
	}
	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with gcs_cache.")
	}
	opts := make([]option.ClientOption, 0)
	if *credentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(*credentialsFile))
	}
	gcsCache, err := NewGCSCache(*bucket, *projectID, *ttlDays, opts...)
	if err != nil {
		return status.InternalErrorf("Error configuring GCS cache: %s", err)
	}
	env.SetCache(gcsCache)
	return nil
}

func NewGCSCache(bucketName, projectID string, ageInDays int64, opts ...option.ClientOption) (*GCSCache, error) {
	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	g := &GCSCache{
		gcsClient: gcsClient,
		projectID: projectID,
		ttlInDays: ageInDays,
	}
	if err := g.createBucketIfNotExists(ctx, bucketName); err != nil {
		return nil, err
	}
	if err := g.setBucketTTL(ctx, bucketName, ageInDays); err != nil {
		return nil, err
	}
	log.Printf("Initialized GCS cache with bucket %q, ttl (days): %d", bucketName, ageInDays)
	return g, nil
}

func (g *GCSCache) bucketExists(ctx context.Context, bucketName string) (bool, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	_, err := g.gcsClient.Bucket(bucketName).Attrs(ctx)
	return err == nil, err
}
func (g *GCSCache) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	if exists, _ := g.bucketExists(ctx, bucketName); !exists {
		log.Printf("Creating storage bucket: %s", bucketName)
		g.bucketHandle = g.gcsClient.Bucket(bucketName)
		ctx, spn := tracing.StartSpan(ctx)
		defer spn.End()
		return g.bucketHandle.Create(ctx, g.projectID, nil)
	}
	g.bucketHandle = g.gcsClient.Bucket(bucketName)
	return nil
}

func (g *GCSCache) setBucketTTL(ctx context.Context, bucketName string, ageInDays int64) error {
	traceCtx, spn := tracing.StartSpan(ctx)
	spn.SetName("Attrs for GCSCache SetBucketTTL")
	attrs, err := g.gcsClient.Bucket(bucketName).Attrs(traceCtx)
	spn.End()
	if err != nil {
		return err
	}
	for _, rule := range attrs.Lifecycle.Rules {
		if rule.Condition.AgeInDays == ageInDays &&
			rule.Action.Type == storage.DeleteAction {
			return nil
		}
	}
	lc := storage.Lifecycle{
		Rules: []storage.LifecycleRule{
			{
				Condition: storage.LifecycleCondition{
					AgeInDays: ageInDays,
				},
				Action: storage.LifecycleAction{
					Type: storage.DeleteAction,
				},
			},
		},
	}
	traceCtx, spn = tracing.StartSpan(ctx)
	spn.SetName("Update for GCSCache SetBucketTTL")
	defer spn.End()
	// Update the bucket TTL, regardless of whatever value is set.
	_, err = g.gcsClient.Bucket(bucketName).Update(traceCtx, storage.BucketAttrsToUpdate{Lifecycle: &lc})
	return err
}

func (g *GCSCache) key(ctx context.Context, d *repb.Digest) (string, error) {
	hash, err := digest.Validate(d)
	if err != nil {
		return "", err
	}
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", err
	}
	return userPrefix + g.prefix + hash, nil
}

func (g *GCSCache) WithIsolation(ctx context.Context, cacheType interfaces.CacheTypeDeprecated, remoteInstanceName string) (interfaces.Cache, error) {
	newPrefix := filepath.Join(remoteInstanceName, cacheType.Prefix())
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}

	return &GCSCache{
		gcsClient:    g.gcsClient,
		bucketHandle: g.bucketHandle,
		projectID:    g.projectID,
		ttlInDays:    g.ttlInDays,
		prefix:       newPrefix,
	}, nil
}

func (g *GCSCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	k, err := g.key(ctx, d)
	if err != nil {
		return nil, err
	}

	reader, err := g.bucketHandle.Object(k).NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
		}
		return nil, err
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	_, spn := tracing.StartSpan(ctx)
	b, err := io.ReadAll(reader)
	spn.End()
	timer.ObserveGet(len(b), err)
	// Note, if we decide to retry reads in the future, be sure to
	// add a new metric for retry count.
	return b, err
}

func (g *GCSCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest][]byte, len(digests))
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				data, err := g.Get(ctx, d)
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

func swallowGCSAlreadyExistsError(err error) error {
	if err != nil {
		if gerr, ok := err.(*googleapi.Error); ok {
			if gerr.Code == http.StatusPreconditionFailed {
				return nil
			}
			// When building with some languages, like Java, certain
			// files like MANIFEST files which are identical across
			// all actions can be uploaded. Many concurrent actions
			// means that these files can be written simultaneously
			// which triggers http.StatusTooManyRequests. Because
			// the cache is a CAS, we assume that writing the same
			// hash over and over again is just writing the same
			// file, and we swallow the error here.
			if gerr.Code == http.StatusTooManyRequests {
				return nil
			}
		}
	}
	return err
}

func (g *GCSCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	k, err := g.key(ctx, d)
	if err != nil {
		return err
	}
	numAttempts := 0
	for {
		obj := g.bucketHandle.Object(k)
		writer := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
		setChunkSize(d, writer)
		timer := cache_metrics.NewCacheTimer(cacheLabels)
		_, spn := tracing.StartSpan(ctx)
		_, err = writer.Write(data)
		spn.End()
		if err == nil {
			err = swallowGCSAlreadyExistsError(writer.Close())
		}
		timer.ObserveSet(len(data), err)
		numAttempts++
		if !isRetryableGCSError(err) || numAttempts > maxNumRetries {
			break
		}
	}
	cache_metrics.RecordSetRetries(cacheLabels, numAttempts-1)
	return err
}

func (g *GCSCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	eg, ctx := errgroup.WithContext(ctx)

	for d, data := range kvs {
		setFn := func(d *repb.Digest, data []byte) {
			eg.Go(func() error {
				return g.Set(ctx, d, data)
			})
		}
		setFn(d, data)
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (g *GCSCache) Delete(ctx context.Context, d *repb.Digest) error {
	k, err := g.key(ctx, d)
	if err != nil {
		return err
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	ctx, spn := tracing.StartSpan(ctx)
	err = g.bucketHandle.Object(k).Delete(ctx)
	spn.End()
	timer.ObserveDelete(err)
	// Note, if we decide to retry deletions in the future, be sure to
	// add a new metric for retry count.
	if errors.Is(err, storage.ErrObjectNotExist) {
		return status.NotFoundErrorf("digest %s/%d not found in gcs_cache: %s", d.GetHash(), d.GetSizeBytes(), err.Error())
	}
	return err
}

func (g *GCSCache) bumpTTLIfStale(ctx context.Context, key string, t time.Time) bool {
	if g.ttlInDays == 0 || int64(time.Since(t).Hours()) < 24*g.ttlInDays/2 {
		return true
	}
	obj := g.bucketHandle.Object(key)
	ctx, spn := tracing.StartSpan(ctx)
	_, err := obj.CopierFrom(obj).Run(ctx)
	spn.End()
	if err == storage.ErrObjectNotExist {
		return false
	}
	if err != nil {
		log.Printf("Error bumping TTL for key %s: %s", key, err.Error())
	}
	return true
}

func (g *GCSCache) metadata(ctx context.Context, d *repb.Digest) (*storage.ObjectAttrs, error) {
	k, err := g.key(ctx, d)
	if err != nil {
		return nil, err
	}
	finalErr := error(nil)
	numAttempts := 0
	for {
		timer := cache_metrics.NewCacheTimer(cacheLabels)
		ctx, spn := tracing.StartSpan(ctx)
		attrs, err := g.bucketHandle.Object(k).Attrs(ctx)
		spn.End()
		timer.ObserveContains(err)
		numAttempts++
		finalErr = err
		if err == storage.ErrObjectNotExist {
			return nil, nil
		} else if err == nil {
			return attrs, nil
		} else if isRetryableGCSError(err) {
			log.Printf("Retrying GCS exists, err: %s", err.Error())
			continue
		}
		break
	}
	cache_metrics.RecordSetRetries(cacheLabels, numAttempts-1)
	return nil, finalErr
}

func (g *GCSCache) ContainsDeprecated(ctx context.Context, d *repb.Digest) (bool, error) {
	metadata, err := g.metadata(ctx, d)
	if err != nil || metadata == nil {
		return false, err
	}

	// Bump TTL to ensure that referenced blobs are available and will be for some period of time afterwards,
	// as specified by the protocol description
	k, err := g.key(ctx, d)
	if err != nil {
		return false, err
	}
	bumped := g.bumpTTLIfStale(ctx, k, metadata.Created)
	if bumped {
		return true, nil
	}
	return false, nil
}

// TODO(buildbuddy-internal#1485) - Add last access time
func (g *GCSCache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	metadata, err := g.metadata(ctx, d)
	if err != nil {
		return nil, err
	}
	if metadata == nil {
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}
	return &interfaces.CacheMetadata{
		SizeBytes:          metadata.Size,
		LastModifyTimeUsec: metadata.Updated.UnixMicro(),
	}, nil
}

func (g *GCSCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	lock := sync.RWMutex{} // protects(missing)
	var missing []*repb.Digest
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				exists, err := g.ContainsDeprecated(ctx, d)
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

func (g *GCSCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	k, err := g.key(ctx, d)
	if err != nil {
		return nil, err
	}
	ctx, spn := tracing.StartSpan(ctx)
	if limit == 0 {
		limit = -1
	}
	reader, err := g.bucketHandle.Object(k).NewRangeReader(ctx, offset, limit)
	spn.End()
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
		}
		return nil, err
	}
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	// rely on google's internal tracing to capture read calls from the returned reader
	return io.NopCloser(timer.NewInstrumentedReader(reader, d.GetSizeBytes())), nil
}

func isRetryableGCSError(err error) bool {
	if err != nil {
		if gerr, ok := err.(*googleapi.Error); ok {
			switch gerr.Code {
			case http.StatusServiceUnavailable: // 503
			case http.StatusBadGateway: // 502
				log.Printf("Saw a retryable error: %s", err.Error())
				return true
			default:
				return false
			}
		}
	}
	return false
}

type gcsDedupingWriteCloser struct {
	io.WriteCloser
	timer *cache_metrics.CacheTimer
	size  int64
}

func (wc *gcsDedupingWriteCloser) Write(in []byte) (int, error) {
	n, err := wc.WriteCloser.Write(in)

	numRetries := 0
	for isRetryableGCSError(err) && numRetries < maxNumRetries {
		log.Printf("Retrying GCS write after error: %s", err.Error())
		numRetries++
		n, err = wc.WriteCloser.Write(in)
	}
	cache_metrics.RecordWriteRetries(cacheLabels, numRetries)

	return n, err
}

func (wc *gcsDedupingWriteCloser) Close() error {
	return swallowGCSAlreadyExistsError(wc.WriteCloser.Close())
}

func setChunkSize(d *repb.Digest, w *storage.Writer) {
	switch size := d.GetSizeBytes(); {
	case size < 8*1000*1000:
		{
			w.ChunkSize = int(size)
		}
	default:
		w.ChunkSize = googleapi.DefaultUploadChunkSize
	}
}

func (g *GCSCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	k, err := g.key(ctx, d)
	if err != nil {
		return nil, err
	}
	obj := g.bucketHandle.Object(k)
	writer := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	setChunkSize(d, writer)
	timer := cache_metrics.NewCacheTimer(cacheLabels)
	return &gcsDedupingWriteCloser{
		WriteCloser: writer,
		timer:       timer,
		size:        d.GetSizeBytes(),
	}, nil
}

func (g *GCSCache) Start() error {
	return nil
}

func (g *GCSCache) Stop() error {
	return nil
}
