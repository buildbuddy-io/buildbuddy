package migration_cache

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

var (
	cacheMigrationConfig = flagutil.New("cache.migration", MigrationConfig{}, "Config to specify the details of a cache migration")
)

type MigrationCache struct {
	Src  interfaces.Cache
	Dest interfaces.Cache
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
	mc := &MigrationCache{
		Src:  srcCache,
		Dest: destCache,
	}

	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with migration_cache. If running a migration, all cache configs" +
			" should be nested under the cache.migration block.")
	}
	env.SetCache(mc)

	return nil
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

func (mc MigrationCache) WithIsolation(ctx context.Context, cacheType interfaces.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

func (mc MigrationCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	return false, status.UnimplementedError("not yet implemented")
}

func (mc MigrationCache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

func (mc MigrationCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

func (mc MigrationCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

func (mc MigrationCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

func (mc MigrationCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	return status.UnimplementedError("not yet implemented")
}

func (mc MigrationCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	return status.UnimplementedError("not yet implemented")
}

func (mc MigrationCache) Delete(ctx context.Context, d *repb.Digest) error {
	return status.UnimplementedError("not yet implemented")
}

func (mc MigrationCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	return nil, status.UnimplementedError("not yet implemented")
}

func (mc MigrationCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	return nil, status.UnimplementedError("not yet implemented")
}
