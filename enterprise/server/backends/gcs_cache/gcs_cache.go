package gcs_cache

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/cache_metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
	ttlInDays    int64
	prefix       string
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

func (g *GCSCache) createBucketIfNotExists(ctx context.Context, bucketName string) error {
	if _, err := g.gcsClient.Bucket(bucketName).Attrs(ctx); err != nil {
		log.Printf("Creating storage bucket: %s", bucketName)
		g.bucketHandle = g.gcsClient.Bucket(bucketName)
		return g.bucketHandle.Create(ctx, g.projectID, nil)
	}
	g.bucketHandle = g.gcsClient.Bucket(bucketName)
	return nil
}

func (g *GCSCache) setBucketTTL(ctx context.Context, bucketName string, ageInDays int64) error {
	attrs, err := g.gcsClient.Bucket(bucketName).Attrs(ctx)
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
			storage.LifecycleRule{
				Condition: storage.LifecycleCondition{
					AgeInDays: ageInDays,
				},
				Action: storage.LifecycleAction{
					Type: storage.DeleteAction,
				},
			},
		},
	}
	// Update the bucket TTL, regardless of whatever value is set.
	_, err = g.gcsClient.Bucket(bucketName).Update(ctx, storage.BucketAttrsToUpdate{Lifecycle: &lc})
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

func (g *GCSCache) WithPrefix(prefix string) interfaces.Cache {
	newPrefix := filepath.Join(append(filepath.SplitList(g.prefix), prefix)...)
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}

	return &GCSCache{
		gcsClient:    g.gcsClient,
		bucketHandle: g.bucketHandle,
		projectID:    g.projectID,
		ttlInDays:    g.ttlInDays,
		prefix:       newPrefix,
	}
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
	b, err := ioutil.ReadAll(reader)
	timer.EndGet(len(b), err)
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
		if _, err = writer.Write(data); err == nil {
			err = swallowGCSAlreadyExistsError(writer.Close())
		}
		timer.EndSet(len(data), err)
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
	err = g.bucketHandle.Object(k).Delete(ctx)
	timer.EndDelete(err)
	// Note, if we decide to retry deletions in the future, be sure to
	// add a new metric for retry count.
	return err
}

func (g *GCSCache) bumpTTLIfStale(ctx context.Context, key string, t time.Time) bool {
	if int64(time.Since(t).Hours()) < 24*g.ttlInDays/2 {
		return true
	}
	obj := g.bucketHandle.Object(key)
	_, err := obj.CopierFrom(obj).Run(ctx)
	if err == storage.ErrObjectNotExist {
		return false
	}
	if err != nil {
		log.Printf("Error bumping TTL for key %s: %s", key, err.Error())
	}
	return true
}

func (g *GCSCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	k, err := g.key(ctx, d)
	if err != nil {
		return false, err
	}
	finalErr := error(nil)
	numAttempts := 0
	for {
		timer := cache_metrics.NewCacheTimer(cacheLabels)
		attrs, err := g.bucketHandle.Object(k).Attrs(ctx)
		timer.EndContains(err)
		numAttempts++
		finalErr = err
		if err == storage.ErrObjectNotExist {
			return false, nil
		} else if err == nil {
			return g.bumpTTLIfStale(ctx, k, attrs.Created), nil
		} else if isRetryableGCSError(err) {
			log.Printf("Retrying GCS exists, err: %s", err.Error())
			continue
		}
		break
	}
	cache_metrics.RecordSetRetries(cacheLabels, numAttempts-1)
	return false, finalErr
}

func (g *GCSCache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest]bool, len(digests))
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				exists, err := g.Contains(ctx, d)
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

func (g *GCSCache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.Reader, error) {
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
	return timer.NewInstrumentedReader(reader, d.GetSizeBytes()), nil
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
	size int64
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
		timer: timer,
		size: d.GetSizeBytes(),
	}, nil
}

func (g *GCSCache) Start() error {
	return nil
}

func (g *GCSCache) Stop() error {
	return nil
}
