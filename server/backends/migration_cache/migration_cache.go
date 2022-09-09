package migration_cache

import (
	"context"
	"io"
	"math/rand"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

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
	mc := NewMigrationCache(srcCache, destCache, cacheMigrationConfig.DoubleReadPercentage, cacheMigrationConfig.LogNotFoundErrors)

	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with migration_cache. If running a migration, all cache configs" +
			" should be nested under the cache.migration block.")
	}
	env.SetCache(mc)

	return nil
}

func NewMigrationCache(srcCache interfaces.Cache, destCache interfaces.Cache, doubleReadPercentage float64, logNotFoundErrs bool) *MigrationCache {
	return &MigrationCache{
		src:                  srcCache,
		dest:                 destCache,
		doubleReadPercentage: doubleReadPercentage,
		logNotFoundErrors:    logNotFoundErrs,
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
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return c.Stop()
	})
	return c, nil
}

func (mc *MigrationCache) WithIsolation(ctx context.Context, cacheType interfaces.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

func (mc *MigrationCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	return false, status.UnimplementedError("not yet implemented")
}

func (mc *MigrationCache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

func (mc *MigrationCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

func (mc *MigrationCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

func (mc *MigrationCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	return status.UnimplementedError("not yet implemented")
}

func (mc *MigrationCache) Delete(ctx context.Context, d *repb.Digest) error {
	return status.UnimplementedError("not yet implemented")
}

func (mc *MigrationCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

func (mc *MigrationCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

// TODO(Maggie): Add copying logic
func (mc *MigrationCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error
	var srcBuf []byte

	eg.Go(func() error {
		srcBuf, srcErr = mc.src.Get(gctx, d)
		return srcErr
	})

	// Double read some proportion to guarantee that data is consistent between caches
	doubleRead := rand.Float64() <= mc.doubleReadPercentage
	if doubleRead {
		eg.Go(func() error {
			_, dstErr = mc.dest.Get(gctx, d)
			return nil // we don't care about the return error from this cache
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if dstErr != nil {
		if mc.logNotFoundErrors || !status.IsNotFoundError(dstErr) {
			log.Warningf("Double read of %q failed. src err %s, dest err %s", d, srcErr, dstErr)
		}
	}

	// Return data from source cache
	return srcBuf, srcErr
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
