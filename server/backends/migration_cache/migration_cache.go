package migration_cache

import (
	"context"
	"io"
	"reflect"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

type MigrationCache struct {
	Src  interfaces.Cache
	Dest interfaces.Cache
}

func Register(env environment.Env, cfg config.MigrationConfig) error {
	log.Infof("Registering Migration Cache")

	srcCache, err := getCacheFromConfig(env, cfg.Src)
	if err != nil {
		return err
	}
	destCache, err := getCacheFromConfig(env, cfg.Dest)
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

func validateCacheConfig(config config.CacheConfig) error {
	numConfigs := 0
	v := reflect.ValueOf(config)
	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).IsNil() {
			numConfigs++
		}
	}

	if numConfigs != 1 {
		return status.FailedPreconditionError("exactly one config must be set")
	}

	return nil
}

func getCacheFromConfig(env environment.Env, cfg config.CacheConfig) (interfaces.Cache, error) {
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

func diskCacheFromConfig(env environment.Env, cfg *config.DiskCacheConfig) (*disk_cache.DiskCache, error) {
	opts := &disk_cache.Options{
		RootDirectory:     cfg.RootDirectory,
		Partitions:        cfg.Partitions,
		PartitionMappings: cfg.PartitionMappings,
		UseV2Layout:       cfg.UseV2Layout,
	}
	return disk_cache.NewDiskCache(env, opts, cache_config.MaxSizeBytes())
}

func pebbleCacheFromConfig(env environment.Env, cfg *config.PebbleCacheConfig) (*pebble_cache.PebbleCache, error) {
	opts := pebble_cache.OptionsFromConstructor(pebble_cache.OptionsConstructor{
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
	})
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
	//TODO implement me
	panic("implement me")
}

func (mc MigrationCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (mc MigrationCache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	//TODO implement me
	panic("implement me")
}

func (mc MigrationCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	//TODO implement me
	panic("implement me")
}

func (mc MigrationCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (mc MigrationCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (mc MigrationCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	//TODO implement me
	panic("implement me")
}

func (mc MigrationCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (mc MigrationCache) Delete(ctx context.Context, d *repb.Digest) error {
	//TODO implement me
	panic("implement me")
}

func (mc MigrationCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	//TODO implement me
	panic("implement me")
}

func (mc MigrationCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	//TODO implement me
	panic("implement me")
}
