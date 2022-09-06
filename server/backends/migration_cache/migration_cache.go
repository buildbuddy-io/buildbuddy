package migration_cache

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"sync"

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
	Src                  interfaces.Cache
	Dest                 interfaces.Cache
	DoubleReadPercentage float64

	mu sync.RWMutex
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
	mc := NewMigrationCache(srcCache, destCache, cacheMigrationConfig.DoubleReadPercentage)

	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with migration_cache. If running a migration, all cache configs" +
			" should be nested under the cache.migration block.")
	}
	env.SetCache(mc)

	return nil
}

func NewMigrationCache(srcCache interfaces.Cache, destCache interfaces.Cache, doubleReadPercentage float64) *MigrationCache {
	return &MigrationCache{
		Src:                  srcCache,
		Dest:                 destCache,
		DoubleReadPercentage: doubleReadPercentage,
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

type getResult struct {
	fromSrcCache bool
	data         []byte
	err          error
}

func (mc *MigrationCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	resultChan := make(chan getResult)
	mc.mu.Lock()
	go func() {
		data, err := mc.Src.Get(ctx, d)
		resultChan <- getResult{
			fromSrcCache: true,
			data:         data,
			err:          err,
		}
	}()

	// Double read some proportion to guarantee that data is consistent between caches
	shouldDoubleRead := rand.Float64() <= mc.DoubleReadPercentage
	var srcResult getResult
	if shouldDoubleRead {
		go func() {
			data, err := mc.Dest.Get(ctx, d)
			resultChan <- getResult{
				fromSrcCache: false,
				data:         data,
				err:          err,
			}
		}()

		r1, r2 := <-resultChan, <-resultChan
		mc.mu.Unlock()
		srcResult = compareDoubleReads(r1, r2)
	} else {
		srcResult = <-resultChan
		mc.mu.Unlock()
	}

	// Return data from source cache
	return srcResult.data, srcResult.err
}

// compareDoubleReads compares read results from two caches and logs if there are errors or discrepancies
// Returns data from the source cache
func compareDoubleReads(r1 getResult, r2 getResult) getResult {
	var srcResult getResult
	var destResult getResult
	if r1.fromSrcCache {
		srcResult = r1
		destResult = r2
	} else {
		srcResult = r2
		destResult = r1
	}

	if srcResult.err != nil || destResult.err != nil {
		log.Errorf("Migration double read err: src err: %v, dest err: %v", srcResult.err, destResult.err)
	} else if !bytes.Equal(srcResult.data, destResult.data) {
		log.Infof("Migration double read err: src data is %v, dest data is %v", string(srcResult.data), string(destResult.data))
	}

	return srcResult
}

type setResult struct {
	fromSrcCache bool
	err          error
}

func (mc *MigrationCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	// Double write data to both caches
	resultChan := make(chan setResult)
	mc.mu.Lock()
	go func() {
		err := mc.Src.Set(ctx, d, data)
		resultChan <- setResult{
			fromSrcCache: true,
			err:          err,
		}
	}()
	go func() {
		err := mc.Dest.Set(ctx, d, data)
		resultChan <- setResult{
			fromSrcCache: false,
			err:          err,
		}
	}()

	r1, r2 := <-resultChan, <-resultChan
	mc.mu.Unlock()

	var srcResult setResult
	var destResult setResult
	if r1.fromSrcCache {
		srcResult = r1
		destResult = r2
	} else {
		srcResult = r2
		destResult = r1
	}

	if srcResult.err != nil && destResult.err == nil {
		// If error during write to source cache (source of truth), must delete from destination cache
		deleteErr := mc.Dest.Delete(ctx, d)
		if deleteErr != nil {
			log.Errorf("Migration double write err: src write of digest %v failed, but could not delete from dest cache: %s", d, deleteErr)
		}
	} else if destResult.err != nil {
		log.Errorf("Migration double write err: failure writing digest %v to dest cache: %s", d, destResult.err)
	}

	return srcResult.err
}
