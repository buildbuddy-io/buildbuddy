package migration_cache

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

var (
	cacheMigrationConfig = flagutil.New("cache.migration", MigrationConfig{}, "Config to specify the details of a cache migration")
)

type MigrationCache struct {
	src                  interfaces.Cache
	dest                 interfaces.Cache
	doubleReadPercentage float64
	logNotFoundErrors    bool

	eg       *errgroup.Group
	quitChan chan struct{}

	maxCopiesPerSec             int
	copyChan                    chan *copyData
	copyChanFullWarningInterval time.Duration
	numCopiesDropped            *int64

	// TODO(Maggie): Clean up these fields when removing WithIsolation
	// These fields describe the current isolation, as set by WithIsolation
	remoteInstanceName string
	cacheType          resource.CacheType
}

func Register(env environment.Env) error {
	if cacheMigrationConfig.Src == nil || cacheMigrationConfig.Dest == nil {
		return nil
	}
	log.Infof("Registering Migration Cache")

	srcCache, err := getCacheFromConfig(env, *cacheMigrationConfig.Src)
	if err != nil {
		return err
	}
	destCache, err := getCacheFromConfig(env, *cacheMigrationConfig.Dest)
	if err != nil {
		return err
	}
	cacheMigrationConfig.SetConfigDefaults()
	mc := NewMigrationCache(cacheMigrationConfig, srcCache, destCache)

	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with migration_cache. If running a migration, all cache configs" +
			" should be nested under the cache.migration block.")
	}
	env.SetCache(mc)

	mc.Start()
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return mc.Stop()
	})

	return nil
}

func NewMigrationCache(migrationConfig *MigrationConfig, srcCache interfaces.Cache, destCache interfaces.Cache) *MigrationCache {
	zero := int64(0)
	return &MigrationCache{
		src:                         srcCache,
		dest:                        destCache,
		doubleReadPercentage:        migrationConfig.DoubleReadPercentage,
		logNotFoundErrors:           migrationConfig.LogNotFoundErrors,
		copyChan:                    make(chan *copyData, migrationConfig.CopyChanBufferSize),
		maxCopiesPerSec:             migrationConfig.MaxCopiesPerSec,
		eg:                          &errgroup.Group{},
		copyChanFullWarningInterval: time.Duration(migrationConfig.CopyChanFullWarningIntervalMin) * time.Minute,
		numCopiesDropped:            &zero,
		remoteInstanceName:          "",
		cacheType:                   resource.CacheType_CAS,
	}
}

func validateCacheConfig(config CacheConfig) error {
	if config.PebbleConfig != nil && config.DiskConfig != nil {
		return status.FailedPreconditionError("only one cache config can be set")
	} else if config.PebbleConfig == nil && config.DiskConfig == nil {
		return status.FailedPreconditionError("a cache config must be set")
	}

	return nil
}

func getCacheFromConfig(env environment.Env, cfg CacheConfig) (interfaces.Cache, error) {
	err := validateCacheConfig(cfg)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("error validating migration cache config: %s", err)
	}

	if cfg.DiskConfig != nil {
		c, err := diskCacheFromConfig(env, cfg.DiskConfig)
		if err != nil {
			return nil, err
		}
		return c, nil
	} else if cfg.PebbleConfig != nil {
		c, err := pebbleCacheFromConfig(env, cfg.PebbleConfig)
		if err != nil {
			return nil, err
		}
		return c, nil
	}

	return nil, status.FailedPreconditionErrorf("error getting cache from migration config: no valid cache types")
}

func diskCacheFromConfig(env environment.Env, cfg *DiskCacheConfig) (*disk_cache.DiskCache, error) {
	opts := &disk_cache.Options{
		RootDirectory:     cfg.RootDirectory,
		Partitions:        cfg.Partitions,
		PartitionMappings: cfg.PartitionMappings,
		UseV2Layout:       cfg.UseV2Layout,
	}
	return disk_cache.NewDiskCache(env, opts, cache_config.MaxSizeBytes())
}

func pebbleCacheFromConfig(env environment.Env, cfg *PebbleCacheConfig) (*pebble_cache.PebbleCache, error) {
	opts := &pebble_cache.Options{
		RootDirectory:          cfg.RootDirectory,
		Partitions:             cfg.Partitions,
		PartitionMappings:      cfg.PartitionMappings,
		IsolateByGroupIDs:      cfg.IsolateByGroupIDs,
		MaxSizeBytes:           cfg.MaxSizeBytes,
		BlockCacheSizeBytes:    cfg.BlockCacheSizeBytes,
		MaxInlineFileSizeBytes: cfg.MaxInlineFileSizeBytes,
		AtimeUpdateThreshold:   cfg.AtimeUpdateThreshold,
		AtimeWriteBatchSize:    cfg.AtimeWriteBatchSize,
		AtimeBufferSize:        cfg.AtimeBufferSize,
		MinEvictionAge:         cfg.MinEvictionAge,
	}
	c, err := pebble_cache.NewPebbleCache(env, opts)
	if err != nil {
		return nil, err
	}

	c.Start()
	return c, nil
}

func (mc *MigrationCache) WithIsolation(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	return mc.withIsolation(ctx, cacheType, remoteInstanceName)
}

func (mc *MigrationCache) withIsolation(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string) (*MigrationCache, error) {
	srcCache, err := mc.src.WithIsolation(ctx, cacheType, remoteInstanceName)
	if err != nil {
		return nil, errors.WithMessage(err, "cannot get src cache with isolation")
	}
	destCache, err := mc.dest.WithIsolation(ctx, cacheType, remoteInstanceName)
	if err != nil {
		return nil, errors.WithMessage(err, "cannot get dest cache with isolation")
	}

	clone := *mc
	clone.src = srcCache
	clone.dest = destCache
	clone.remoteInstanceName = remoteInstanceName
	clone.cacheType = cacheType

	return &clone, nil
}

func (mc *MigrationCache) Contains(ctx context.Context, r *resource.ResourceName) (bool, error) {
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error
	var srcContains, dstContains bool

	eg.Go(func() error {
		srcContains, srcErr = mc.src.Contains(gctx, r)
		return srcErr
	})

	doubleRead := rand.Float64() <= mc.doubleReadPercentage
	if doubleRead {
		eg.Go(func() error {
			dstContains, dstErr = mc.dest.Contains(gctx, r)
			return nil // we don't care about the return error from this cache
		})
	}

	if err := eg.Wait(); err != nil {
		return false, err
	}

	if dstErr != nil {
		log.Warningf("Migration dest %v contains failed: %s", r.GetDigest(), dstErr)
	} else if doubleRead && srcContains != dstContains {
		metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "contains"}).Inc()
		if mc.logNotFoundErrors || (dstContains && !srcContains) {
			log.Warningf("Migration digest %v src contains %v, dest contains %v", r.GetDigest(), srcContains, dstContains)
		}
	}

	return srcContains, srcErr
}

func (mc *MigrationCache) ContainsDeprecated(ctx context.Context, d *repb.Digest) (bool, error) {
	return mc.Contains(ctx, &resource.ResourceName{
		Digest:       d,
		InstanceName: mc.remoteInstanceName,
		Compressor:   repb.Compressor_IDENTITY,
		CacheType:    mc.cacheType,
	})
}

func (mc *MigrationCache) Metadata(ctx context.Context, r *resource.ResourceName) (*interfaces.CacheMetadata, error) {
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error
	var srcMetadata *interfaces.CacheMetadata

	eg.Go(func() error {
		srcMetadata, srcErr = mc.src.Metadata(gctx, r)
		return srcErr
	})

	doubleRead := rand.Float64() <= mc.doubleReadPercentage
	if doubleRead {
		eg.Go(func() error {
			_, dstErr = mc.dest.Metadata(gctx, r)
			return nil // we don't care about the return error from this cache
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if dstErr != nil && (mc.logNotFoundErrors || !status.IsNotFoundError(dstErr)) {
		log.Warningf("Migration dest %v metadata failed: %s", r.GetDigest(), dstErr)
	}
	if status.IsNotFoundError(dstErr) {
		metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "metadata"}).Inc()
	}

	return srcMetadata, srcErr
}

func (mc *MigrationCache) MetadataDeprecated(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	return mc.Metadata(ctx, &resource.ResourceName{
		Digest:       d,
		InstanceName: mc.remoteInstanceName,
		Compressor:   repb.Compressor_IDENTITY,
		CacheType:    mc.cacheType,
	})
}

func (mc *MigrationCache) FindMissing(ctx context.Context, resources []*resource.ResourceName) ([]*repb.Digest, error) {
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error
	var srcMissing, dstMissing []*repb.Digest
	doubleRead := rand.Float64() <= mc.doubleReadPercentage

	// Some implementations of FindMissing sort the resources slice, which can cause a race condition if both
	// the src and dest cache try to sort at the same time. Copy the slice to prevent this
	var resourcesCopy []*resource.ResourceName
	if doubleRead {
		resourcesCopy = make([]*resource.ResourceName, len(resources))
		copy(resourcesCopy, resources)
	}

	eg.Go(func() error {
		srcMissing, srcErr = mc.src.FindMissing(gctx, resources)
		return srcErr
	})

	if doubleRead {
		eg.Go(func() error {
			dstMissing, dstErr = mc.dest.FindMissing(gctx, resourcesCopy)
			return nil // we don't care about the return error from this cache
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if dstErr != nil {
		log.Warningf("Migration dest FindMissing %v failed: %s", resources, dstErr)
	}
	if doubleRead {
		missingOnlyInDest, missingOnlyInSrc := digest.Diff(srcMissing, dstMissing)

		if len(missingOnlyInSrc) == 0 && len(missingOnlyInDest) == 0 {
			metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "findMissingMatch"}).Inc()
		} else {
			metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "findMissing"}).Inc()
			if mc.logNotFoundErrors || len(missingOnlyInSrc) != 0 {
				log.Warningf("Migration FindMissing diff for digests %v: src %v, dest %v", resources, srcMissing, dstMissing)
			}

			instanceName := resources[0].GetInstanceName()
			cacheType := resources[0].GetCacheType()
			for _, d := range missingOnlyInDest {
				r := &resource.ResourceName{
					Digest:       d,
					InstanceName: instanceName,
					Compressor:   repb.Compressor_IDENTITY,
					CacheType:    cacheType,
				}
				mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/)
			}
		}
	}

	return srcMissing, srcErr
}

func (mc *MigrationCache) FindMissingDeprecated(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	rns := digest.ResourceNames(mc.cacheType, mc.remoteInstanceName, digests)
	return mc.FindMissing(ctx, rns)
}

func (mc *MigrationCache) GetMulti(ctx context.Context, resources []*resource.ResourceName) (map[*repb.Digest][]byte, error) {
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error
	var srcData map[*repb.Digest][]byte

	eg.Go(func() error {
		srcData, srcErr = mc.src.GetMulti(gctx, resources)
		return srcErr
	})

	doubleRead := rand.Float64() <= mc.doubleReadPercentage
	if doubleRead {
		eg.Go(func() error {
			_, dstErr = mc.dest.GetMulti(gctx, resources)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if dstErr != nil {
		if mc.logNotFoundErrors || !status.IsNotFoundError(dstErr) {
			log.Warningf("Migration dest GetMulti of %v failed: %s", resources, dstErr)
		}
		if status.IsNotFoundError(dstErr) {
			metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "getMulti"}).Inc()
		}
	}

	for _, r := range resources {
		mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/)
	}

	// Return data from source cache
	return srcData, srcErr
}

func (mc *MigrationCache) GetMultiDeprecated(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	rns := digest.ResourceNames(mc.cacheType, mc.remoteInstanceName, digests)
	return mc.GetMulti(ctx, rns)
}

func (mc *MigrationCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error

	// Double write data to both caches
	eg.Go(func() error {
		srcErr = mc.src.SetMulti(gctx, kvs)
		return srcErr
	})

	eg.Go(func() error {
		dstErr = mc.dest.SetMulti(gctx, kvs)
		return nil // don't fail if there's an error from this cache
	})

	if err := eg.Wait(); err != nil {
		if dstErr == nil {
			// If error during write to source cache (source of truth), must delete from destination cache
			mc.deleteMulti(ctx, kvs)
		}
		return err
	}

	if dstErr != nil {
		log.Warningf("Migration dest SetMulti of %v err: %s", kvs, dstErr)
	}

	return srcErr
}

func (mc *MigrationCache) deleteMulti(ctx context.Context, kvs map[*repb.Digest][]byte) {
	eg, gctx := errgroup.WithContext(ctx)
	for d, _ := range kvs {
		dCopy := d
		eg.Go(func() error {
			deleteErr := mc.dest.Delete(gctx, dCopy)
			if deleteErr != nil && !status.IsNotFoundError(deleteErr) {
				log.Warningf("Migration double write err: src write of digest %v failed, but could not delete from dest cache: %s", dCopy, deleteErr)
			}
			return nil
		})
	}
	eg.Wait()
}

func (mc *MigrationCache) Delete(ctx context.Context, d *repb.Digest) error {
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error

	eg.Go(func() error {
		srcErr = mc.src.Delete(gctx, d)
		return srcErr
	})

	eg.Go(func() error {
		dstErr = mc.dest.Delete(gctx, d)
		return nil // don't fail if there's an error from this cache
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	if dstErr != nil && (mc.logNotFoundErrors || !status.IsNotFoundError(dstErr)) {
		log.Warningf("Migration could not delete %v from dest cache: %s", d, dstErr)
	}

	return srcErr
}

type doubleReader struct {
	src  io.ReadCloser
	dest io.ReadCloser
}

func (d *doubleReader) Read(p []byte) (n int, err error) {
	var dstErr error
	eg := &errgroup.Group{}

	if d.dest != nil {
		eg.Go(func() error {
			pCopy := make([]byte, len(p))
			_, dstErr = d.dest.Read(pCopy)
			return nil
		})
	}

	srcN, srcErr := d.src.Read(p)

	eg.Wait()
	if dstErr != nil && dstErr != srcErr {
		log.Warningf("Migration read err, src err: %s, dest err: %s", srcErr, dstErr)
	}

	return srcN, srcErr
}

func (d *doubleReader) Close() error {
	eg := &errgroup.Group{}
	if d.dest != nil {
		eg.Go(func() error {
			dstErr := d.dest.Close()
			if dstErr != nil {
				log.Warningf("Migration dest reader close err: %s", dstErr)
			}
			return nil
		})
	}

	srcErr := d.src.Close()
	eg.Wait()

	return srcErr
}

func (mc *MigrationCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	eg := &errgroup.Group{}
	var dstErr error
	var destReader io.ReadCloser

	doubleRead := rand.Float64() <= mc.doubleReadPercentage
	if doubleRead {
		eg.Go(func() error {
			destReader, dstErr = mc.dest.Reader(ctx, d, offset, limit)
			if dstErr != nil && (mc.logNotFoundErrors || !status.IsNotFoundError(dstErr)) {
				log.Warningf("%v reader failed for dest cache: %s", d, dstErr)
			}
			if status.IsNotFoundError(dstErr) {
				metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "reader"}).Inc()
			}
			if dstErr == nil {
				metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "readerMatch"}).Inc()
			}
			return nil
		})
	}

	srcReader, srcErr := mc.src.Reader(ctx, d, offset, limit)
	eg.Wait()

	if srcErr != nil {
		if destReader != nil {
			err := destReader.Close()
			if err != nil {
				log.Warningf("Migration dest reader close err: %s", err)
			}
		}
		log.Warningf("Migration %v src reader err, doubleRead is %v: %s", d, doubleRead, srcErr)
		return nil, srcErr
	}

	r := &resource.ResourceName{
		Digest:       d,
		InstanceName: mc.remoteInstanceName,
		Compressor:   repb.Compressor_IDENTITY,
		CacheType:    mc.cacheType,
	}
	mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/)

	return &doubleReader{
		src:  srcReader,
		dest: destReader,
	}, nil
}

type doubleWriter struct {
	src          interfaces.CommittedWriteCloser
	dest         interfaces.CommittedWriteCloser
	destDeleteFn func()
}

func (d *doubleWriter) Write(data []byte) (int, error) {
	eg := &errgroup.Group{}
	if d.dest != nil {
		eg.Go(func() error {
			_, dstErr := d.dest.Write(data)
			if dstErr != nil {
				log.Warningf("Migration failure writing digest %v to dest cache: %s", d, dstErr)
			}
			return dstErr
		})
	}

	srcN, srcErr := d.src.Write(data)

	destErr := eg.Wait()
	if srcErr != nil && destErr == nil {
		// If error during write to source cache (source of truth), must delete from destination cache
		d.destDeleteFn()
	}

	return srcN, srcErr
}

func (d *doubleWriter) Commit() error {
	eg := &errgroup.Group{}
	if d.dest != nil {
		eg.Go(func() error {
			dstErr := d.dest.Commit()
			if dstErr != nil {
				log.Warningf("Migration writer commit err: %s", dstErr)
			}
			return nil
		})
	}

	srcErr := d.src.Commit()
	eg.Wait()

	return srcErr
}

func (d *doubleWriter) Close() error {
	eg := &errgroup.Group{}
	if d.dest != nil {
		eg.Go(func() error {
			dstErr := d.dest.Close()
			if dstErr != nil {
				log.Warningf("Migration writer close err: %s", dstErr)
			}
			return nil
		})
	}

	srcErr := d.src.Close()
	eg.Wait()

	return srcErr
}

func (mc *MigrationCache) Writer(ctx context.Context, d *repb.Digest) (interfaces.CommittedWriteCloser, error) {
	eg := &errgroup.Group{}
	var dstErr error
	var destWriter interfaces.CommittedWriteCloser

	eg.Go(func() error {
		destWriter, dstErr = mc.dest.Writer(ctx, d)
		return nil
	})

	srcWriter, srcErr := mc.src.Writer(ctx, d)
	eg.Wait()

	if srcErr != nil {
		if destWriter == nil {
			err := destWriter.Close()
			if err != nil {
				log.Warningf("Migration dest writer close err: %s", err)
			}
		}
		return nil, srcErr
	}
	if dstErr != nil {
		log.Warningf("Migration failure creating dest %v writer: %s", d, dstErr)
	}

	dw := &doubleWriter{
		src:  srcWriter,
		dest: destWriter,
		destDeleteFn: func() {
			deleteErr := mc.dest.Delete(ctx, d)
			if deleteErr != nil && !status.IsNotFoundError(deleteErr) {
				log.Warningf("Migration src write of %v failed, but could not delete from dest cache: %s", d, deleteErr)
			}
		},
	}
	return dw, nil
}

func (mc *MigrationCache) Get(ctx context.Context, r *resource.ResourceName) ([]byte, error) {
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error
	var srcBuf []byte

	eg.Go(func() error {
		srcBuf, srcErr = mc.src.Get(gctx, r)
		return srcErr
	})

	// Double read some proportion to guarantee that data is consistent between caches
	doubleRead := rand.Float64() <= mc.doubleReadPercentage
	if doubleRead {
		eg.Go(func() error {
			_, dstErr = mc.dest.Get(gctx, r)
			return nil // we don't care about the return error from this cache
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if dstErr != nil {
		if mc.logNotFoundErrors || !status.IsNotFoundError(dstErr) {
			log.Warningf("Double read of %q failed. src err %s, dest err %s", r, srcErr, dstErr)
		}
		if status.IsNotFoundError(dstErr) {
			metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "get"}).Inc()
		}
	}

	mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/)

	// Return data from source cache
	return srcBuf, srcErr
}

func (mc *MigrationCache) GetDeprecated(ctx context.Context, d *repb.Digest) ([]byte, error) {
	return mc.Get(ctx, &resource.ResourceName{
		Digest:       d,
		InstanceName: mc.remoteInstanceName,
		Compressor:   repb.Compressor_IDENTITY,
		CacheType:    mc.cacheType,
	})
}

func (mc *MigrationCache) sendNonBlockingCopy(ctx context.Context, r *resource.ResourceName, onlyCopyMissing bool) {
	if onlyCopyMissing {
		alreadyCopied, err := mc.dest.Contains(ctx, r)
		if err != nil {
			log.Warningf("Migration copy err, could not call Contains on dest cache: %s", err)
			return
		}

		if alreadyCopied {
			log.Debugf("Migration skipping copy digest %v, instance %s, cache %v - already copied", r.GetDigest(), r.GetInstanceName(), r.GetCacheType())
			return
		}
	}

	log.Debugf("Migration attempting copy digest %v, instance %s, cache %v", r.GetDigest(), r.GetInstanceName(), r.GetCacheType())
	ctx, cancel := background.ExtendContextForFinalization(ctx, 10*time.Second)
	select {
	case mc.copyChan <- &copyData{
		d:                  r.GetDigest(),
		ctx:                ctx,
		ctxCancel:          cancel,
		remoteInstanceName: r.GetInstanceName(),
		cacheType:          r.GetCacheType(),
	}:
	default:
		log.Debugf("Migration dropping copy digest %v, instance %s, cache %v", r.GetDigest(), r.GetInstanceName(), r.GetCacheType())
		*mc.numCopiesDropped++
		cancel()
	}
}

func (mc *MigrationCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error

	// Double write data to both caches
	eg.Go(func() error {
		srcErr = mc.src.Set(gctx, d, data)
		return srcErr
	})

	eg.Go(func() error {
		dstErr = mc.dest.Set(gctx, d, data)
		return nil // don't fail if there's an error from this cache
	})

	if err := eg.Wait(); err != nil {
		if dstErr == nil {
			// If error during write to source cache (source of truth), must delete from destination cache
			deleteErr := mc.dest.Delete(ctx, d)
			if deleteErr != nil && !status.IsNotFoundError(deleteErr) {
				log.Warningf("Migration double write err: src write of digest %v failed, but could not delete from dest cache: %s", d, deleteErr)
			}
		}
		return err
	}

	if dstErr != nil {
		log.Warningf("Migration double write err: failure writing digest %v to dest cache: %s", d, dstErr)
	}

	return srcErr
}

type copyData struct {
	d                  *repb.Digest
	ctx                context.Context
	ctxCancel          context.CancelFunc
	cacheType          resource.CacheType
	remoteInstanceName string
}

func (mc *MigrationCache) copyDataInBackground() error {
	rateLimiter := rate.NewLimiter(rate.Every(time.Second), mc.maxCopiesPerSec)

	for {
		if err := rateLimiter.Wait(context.Background()); err != nil {
			return err
		}

		select {
		case <-mc.quitChan:
			// Drain copy channel on shutdown
			// We cannot close the channel before draining because there's a chance some cache requests may be
			// concurrently queued as the cache is shutting down, and if we try to enqueue a copy to the closed
			// channel it will panic
			for len(mc.copyChan) > 0 {
				c := <-mc.copyChan
				mc.copy(c)
			}
			return nil
		case c := <-mc.copyChan:
			mc.copy(c)
		}
	}
}

func (mc *MigrationCache) logCopyChanFullInBackground() {
	ticker := time.NewTicker(mc.copyChanFullWarningInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mc.quitChan:
			return
		case <-ticker.C:
			if *mc.numCopiesDropped != 0 {
				log.Warningf("Migration copy chan was full and dropped %d copies in %v. May need to increase buffer size", *mc.numCopiesDropped, mc.copyChanFullWarningInterval)
				zero := int64(0)
				mc.numCopiesDropped = &zero
			}
		}
	}
}

func (mc *MigrationCache) copy(c *copyData) {
	defer c.ctxCancel()

	cache, err := mc.withIsolation(c.ctx, c.cacheType, c.remoteInstanceName)
	if err != nil {
		log.Warningf("Migration copy err: Could not call WithIsolation for type %v instance %s: %s", c.cacheType, c.remoteInstanceName, err)
		return
	}

	srcReader, err := cache.src.Reader(c.ctx, c.d, 0, 0)
	if err != nil {
		if !status.IsNotFoundError(err) {
			log.Warningf("Migration copy err: Could not create %v reader from src cache: %s", c.d, err)
		}
		return
	}
	defer srcReader.Close()

	destWriter, err := cache.dest.Writer(c.ctx, c.d)
	if err != nil {
		log.Warningf("Migration copy err: Could not create %v writer for dest cache: %s", c.d, err)
		return
	}
	defer destWriter.Close()

	if _, err = io.Copy(destWriter, srcReader); err != nil {
		log.Warningf("Migration copy err: Could not create %v writer to dest cache: %s", c.d, err)
	}

	if err := destWriter.Commit(); err != nil {
		log.Warningf("Migration copy err: destination commit failed: %s", err)
	}

	log.Debugf("Migration successfully copied to dest cache: digest %v, instance %s, cache %v", c.d, c.remoteInstanceName, c.cacheType)
}

func (mc *MigrationCache) Start() error {
	mc.quitChan = make(chan struct{}, 0)
	mc.eg.Go(func() error {
		mc.copyDataInBackground()
		return nil
	})
	if mc.copyChanFullWarningInterval > 0 {
		mc.eg.Go(func() error {
			mc.logCopyChanFullInBackground()
			return nil
		})
	}
	return nil
}

func (mc *MigrationCache) Stop() error {
	log.Info("Migration cache beginning shut down")
	defer log.Info("Migration cache successfully shut down")

	close(mc.quitChan)

	// Wait for migration-related channels that use cache resources (like copying) to close before shutting down
	// the caches
	if err := mc.eg.Wait(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var srcShutdownErr, dstShutdownErr error
	if src, canStopSrc := mc.src.(interfaces.StoppableCache); canStopSrc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			srcShutdownErr = src.Stop()
			if srcShutdownErr != nil {
				log.Warningf("Migration src cache shutdown err: %s", srcShutdownErr)
			}
		}()
	}

	if dest, canStopDest := mc.dest.(interfaces.StoppableCache); canStopDest {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dstShutdownErr = dest.Stop()
			if dstShutdownErr != nil {
				log.Warningf("Migration dest cache shutdown err: %s", dstShutdownErr)
			}
		}()
	}

	wg.Wait()

	if srcShutdownErr != nil {
		return srcShutdownErr
	}
	return dstShutdownErr
}
