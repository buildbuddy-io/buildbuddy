package migration_cache

import (
	"context"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

var (
	cacheMigrationConfig = flag.Struct("cache.migration", MigrationConfig{}, "Config to specify the details of a cache migration")
)

type MigrationCache struct {
	env environment.Env

	// The fields in defaultConfigDoNotUseDirectly should not be used by
	// request handlers. Instead use the result of the config() method.
	defaultConfigDoNotUseDirectly config

	logNotFoundErrors bool

	eg       *errgroup.Group
	quitChan chan struct{}

	maxCopiesPerSec             int
	numCopyWorkers              int
	copyChan                    chan *copyData
	copyChanFullWarningInterval time.Duration
	numCopiesDropped            *atomic.Int64

	flagProvider interfaces.ExperimentFlagProvider
}

func Register(env *real_environment.RealEnv) error {
	if cacheMigrationConfig.Src == nil || cacheMigrationConfig.Dest == nil {
		return nil
	}
	log.Info("Registering Migration Cache")

	srcCache, err := getCacheFromConfig(env, *cacheMigrationConfig.Src)
	if err != nil {
		return err
	}
	destCache, err := getCacheFromConfig(env, *cacheMigrationConfig.Dest)
	if err != nil {
		return err
	}
	cacheMigrationConfig.SetConfigDefaults()
	mc := NewMigrationCache(env, cacheMigrationConfig, srcCache, destCache)

	if env.GetCache() != nil {
		log.Warning("Overriding configured cache with migration_cache. If running a migration, all cache configs" +
			" should be nested under the cache.migration block.")
	}
	env.SetCache(mc)

	mc.Start()
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return mc.Stop()
	})

	return nil
}

func NewMigrationCache(env environment.Env, migrationConfig *MigrationConfig, srcCache interfaces.Cache, destCache interfaces.Cache) *MigrationCache {
	return &MigrationCache{
		env: env,
		defaultConfigDoNotUseDirectly: config{
			src:                  srcCache,
			dest:                 destCache,
			asyncDestWrites:      migrationConfig.AsyncDestWrites,
			doubleReadPercentage: migrationConfig.DoubleReadPercentage,
			decompressPercentage: migrationConfig.DecompressPercentage,
		},
		logNotFoundErrors:           migrationConfig.LogNotFoundErrors,
		copyChan:                    make(chan *copyData, migrationConfig.CopyChanBufferSize),
		maxCopiesPerSec:             migrationConfig.MaxCopiesPerSec,
		eg:                          &errgroup.Group{},
		copyChanFullWarningInterval: time.Duration(migrationConfig.CopyChanFullWarningIntervalMin) * time.Minute,
		numCopiesDropped:            &atomic.Int64{},
		numCopyWorkers:              migrationConfig.NumCopyWorkers,
		flagProvider:                env.GetExperimentFlagProvider(),
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
		Name:                        cfg.Name,
		RootDirectory:               cfg.RootDirectory,
		Partitions:                  cfg.Partitions,
		PartitionMappings:           cfg.PartitionMappings,
		MaxSizeBytes:                cfg.MaxSizeBytes,
		BlockCacheSizeBytes:         cfg.BlockCacheSizeBytes,
		MaxInlineFileSizeBytes:      cfg.MaxInlineFileSizeBytes,
		MinBytesAutoZstdCompression: cfg.MinBytesAutoZstdCompression,
		AtimeUpdateThreshold:        cfg.AtimeUpdateThreshold,
		AtimeBufferSize:             cfg.AtimeBufferSize,
		MinEvictionAge:              cfg.MinEvictionAge,
		AverageChunkSizeBytes:       cfg.AverageChunkSizeBytes,
		ClearCacheOnStartup:         cfg.ClearCacheOnStartup,
		ActiveKeyVersion:            cfg.ActiveKeyVersion,
		GCSBucket:                   cfg.GCSConfig.Bucket,
		GCSCredentials:              cfg.GCSConfig.Credentials,
		GCSProjectID:                cfg.GCSConfig.ProjectID,
		GCSAppName:                  cfg.GCSConfig.AppName,
		GCSTTLDays:                  cfg.GCSConfig.TTLDays,
		MinGCSFileSizeBytes:         cfg.GCSConfig.MinGCSFileSizeBytes,
		IncludeMetadataSize:         cfg.IncludeMetadataSize,
		EnableAutoRatchet:           cfg.EnableAutoRatchet,
	}

	c, err := pebble_cache.NewPebbleCache(env, opts)
	if err != nil {
		return nil, err
	}

	c.Start()
	return c, nil
}

// Flags and values for experiments which control the migration cache.
// Exported for testing.
const (
	MigrationCacheConfigFlag      = "migration-cache-config"
	MigrationStateField           = "state"
	AsyncDestWriteField           = "async-dest-write"
	DoubleReadPercentageField     = "double-read-percentage"
	DecompressReadPercentageField = "decompress-read-percentage"

	SrcOnly     = "src-only"
	SrcPrimary  = "src-primary"
	DestOnly    = "dest-only"
	DestPrimary = "dest-primary"
)

// config is the migration configuration for a single request. It defaults to
// the global configuration, but can be overridden with values from the
// experiment flag provider.
type config struct {
	src, dest                                  interfaces.Cache
	asyncDestWrites                            bool
	doubleReadPercentage, decompressPercentage float64
}

// TODO(vanja) either return errors, or remove the error return value.
func (mc *MigrationCache) config(ctx context.Context) (*config, error) {
	c := &config{
		src:                  mc.defaultConfigDoNotUseDirectly.src,
		dest:                 mc.defaultConfigDoNotUseDirectly.dest,
		asyncDestWrites:      mc.defaultConfigDoNotUseDirectly.asyncDestWrites,
		doubleReadPercentage: mc.defaultConfigDoNotUseDirectly.doubleReadPercentage,
		decompressPercentage: mc.defaultConfigDoNotUseDirectly.decompressPercentage,
	}
	if mc.flagProvider == nil {
		return c, nil
	}
	m := mc.flagProvider.Object(ctx, MigrationCacheConfigFlag, nil)
	if m == nil {
		return c, nil
	}

	if v, ok := m[MigrationStateField]; ok {
		if state, ok := v.(string); ok {
			switch state {
			case SrcOnly:
				c.src = mc.defaultConfigDoNotUseDirectly.src
				c.dest = nil
			case SrcPrimary:
				c.src = mc.defaultConfigDoNotUseDirectly.src
				c.dest = mc.defaultConfigDoNotUseDirectly.dest
			case DestPrimary:
				c.src = mc.defaultConfigDoNotUseDirectly.dest
				c.dest = mc.defaultConfigDoNotUseDirectly.src
			case DestOnly:
				c.src = mc.defaultConfigDoNotUseDirectly.dest
				c.dest = nil
			default:
				alert.CtxUnexpectedEvent(ctx, "migration_cache_invalid_config", "Unknown migration cache state: %s", state)
				return c, nil
			}
		} else {
			alert.CtxUnexpectedEvent(ctx, "migration_cache_invalid_config", "MigrationStateField is not a string: %T(%v)", v, v)
			return c, nil
		}
	}
	if v, ok := m[AsyncDestWriteField]; ok {
		if asyncDestWrites, ok := v.(bool); ok {
			c.asyncDestWrites = asyncDestWrites
		} else {
			alert.CtxUnexpectedEvent(ctx, "migration_cache_invalid_config", "AsyncDestWriteField is not a bool: %T(%v)", v, v)
			return c, nil
		}
	}
	if v, ok := m[DoubleReadPercentageField]; ok {
		if doubleReadPercentage, ok := v.(float64); ok {
			c.doubleReadPercentage = doubleReadPercentage
		} else {
			alert.CtxUnexpectedEvent(ctx, "migration_cache_invalid_config", "DoubleReadPercentageField is not a float64: %T(%v)", v, v)
			return c, nil
		}
	}
	if v, ok := m[DecompressReadPercentageField]; ok {
		if decompressPercentage, ok := v.(float64); ok {
			c.decompressPercentage = decompressPercentage
		} else {
			alert.CtxUnexpectedEvent(ctx, "migration_cache_invalid_config", "DecompressReadPercentageField is not a float64: %T(%v)", v, v)
			return c, nil
		}
	}
	return c, nil
}

func (c *config) doubleRead() bool {
	return c.doubleReadPercentage > 0 && rand.Float64() < c.doubleReadPercentage
}

func (c *config) decompressRead() bool {
	return c.decompressPercentage > 0 && rand.Float64() < c.decompressPercentage
}

func groupID(ctx context.Context) string {
	if c, err := claims.ClaimsFromContext(ctx); err == nil {
		return c.GroupID
	}
	return interfaces.AuthAnonymousUser
}

func (mc *MigrationCache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	conf, err := mc.config(ctx)
	if err != nil {
		return false, err
	}
	srcContains, srcErr := conf.src.Contains(ctx, r)

	if srcErr == nil && srcContains && conf.dest != nil && conf.doubleRead() {
		go func() {
			// Timeout is slightly larger than p99.9 latency.
			ctx, cancel := background.ExtendContextForFinalization(ctx, 2*time.Second)
			defer cancel()
			dstContains, dstErr := conf.dest.Contains(ctx, r)
			if dstErr != nil {
				log.CtxWarningf(ctx, "Migration dest %v contains failed: %s", r.GetDigest(), dstErr)
				mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/, conf)
			} else if !dstContains {
				metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{
					metrics.CacheRequestType: "contains",
					metrics.GroupID:          groupID(ctx),
				}).Inc()
				if mc.logNotFoundErrors {
					log.CtxWarningf(ctx, "Migration digest %v src contains, dest does not", r.GetDigest())
				}
				mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/, conf)
			} else {
				metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{
					metrics.CacheRequestType: "contains",
					metrics.GroupID:          groupID(ctx),
				}).Inc()
			}
		}()
	}

	return srcContains, srcErr
}

func (mc *MigrationCache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	conf, err := mc.config(ctx)
	if err != nil {
		return nil, err
	}
	srcMetadata, srcErr := conf.src.Metadata(ctx, r)

	if srcErr == nil && conf.dest != nil && conf.doubleRead() {
		go func() {
			// Timeout is slightly larger than p99.9 latency.
			ctx, cancel := background.ExtendContextForFinalization(ctx, 5*time.Second)
			defer cancel()
			_, dstErr := conf.dest.Metadata(ctx, r)
			if dstErr != nil {
				if status.IsNotFoundError(dstErr) {
					metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{
						metrics.CacheRequestType: "metadata",
						metrics.GroupID:          groupID(ctx),
					}).Inc()
					mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/, conf)
				} else {
					mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/, conf)
				}
				if mc.logNotFoundErrors || !status.IsNotFoundError(dstErr) {
					log.CtxWarningf(ctx, "Migration dest %v metadata failed: %s", r.GetDigest(), dstErr)
				}
			} else {
				metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{
					metrics.CacheRequestType: "metadata",
					metrics.GroupID:          groupID(ctx),
				}).Inc()
			}
		}()
	}

	return srcMetadata, srcErr
}

func (mc *MigrationCache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	conf, err := mc.config(ctx)
	if err != nil {
		return nil, err
	}
	srcMissing, srcErr := conf.src.FindMissing(ctx, resources)

	if srcErr == nil && conf.dest != nil && conf.doubleRead() {
		go func() {
			// Timeout is slightly larger than p99.9 latency.
			ctx, cancel := background.ExtendContextForFinalization(ctx, 2*time.Second)
			defer cancel()
			dstMissing, dstErr := conf.dest.FindMissing(ctx, resources)
			if dstErr != nil {
				log.CtxWarningf(ctx, "Migration dest FindMissing %v failed: %s", resources, dstErr)
				for _, r := range resources {
					mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/, conf)
				}
				return
			}
			missingOnlyInDest, _ := digest.Diff(srcMissing, dstMissing)
			if len(missingOnlyInDest) == 0 {
				metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{
					metrics.CacheRequestType: "findMissing",
					metrics.GroupID:          groupID(ctx),
				}).Inc()
			} else {
				metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{
					metrics.CacheRequestType: "findMissing",
					metrics.GroupID:          groupID(ctx),
				}).Inc()
				if mc.logNotFoundErrors {
					log.CtxWarningf(ctx, "Migration FindMissing diff for digests %v: src %v, dest %v", resources, srcMissing, dstMissing)
				}
				missingSet := make(map[string]struct{}, len(missingOnlyInDest))
				for _, d := range missingOnlyInDest {
					missingSet[d.GetHash()] = struct{}{}
				}
				for _, r := range resources {
					if _, missing := missingSet[r.GetDigest().GetHash()]; missing {
						mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/, conf)
					}
				}
			}
		}()
	}
	return srcMissing, srcErr
}

func (mc *MigrationCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	conf, err := mc.config(ctx)
	if err != nil {
		return nil, err
	}
	srcData, srcErr := conf.src.GetMulti(ctx, resources)
	if conf.dest == nil || srcErr != nil {
		return srcData, srcErr
	}

	go func() {
		// Timeout is slightly larger than p99.9 latency.
		ctx, cancel := background.ExtendContextForFinalization(ctx, 10*time.Second)
		defer cancel()
		doubleRead := conf.doubleRead()
		var dstErr error
		var dstData map[*repb.Digest][]byte
		if doubleRead {
			dstData, dstErr = conf.dest.GetMulti(ctx, resources)
			if dstErr != nil {
				if mc.logNotFoundErrors || !status.IsNotFoundError(dstErr) {
					log.CtxWarningf(ctx, "Migration dest GetMulti of %v failed: %s", resources, dstErr)
				}
				if status.IsNotFoundError(dstErr) {
					metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{
						metrics.CacheRequestType: "getMulti",
						metrics.GroupID:          groupID(ctx),
					}).Inc()
				}
			} else {
				metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{
					metrics.CacheRequestType: "getMulti",
					metrics.GroupID:          groupID(ctx),
				}).Inc()
			}
		}
		if doubleRead && dstErr == nil {
			for _, r := range resources {
				if _, inDest := dstData[r.GetDigest()]; !inDest {
					if srcErr != nil {
						mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/, conf)
					} else if _, inSrc := srcData[r.GetDigest()]; inSrc {
						mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/, conf)
					}
				}
			}
		} else {
			for _, r := range resources {
				mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/, conf)
			}
		}
	}()
	// Return data from source cache
	return srcData, srcErr
}

func (mc *MigrationCache) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
	conf, err := mc.config(ctx)
	if err != nil {
		return err
	}
	if conf.dest == nil {
		return conf.src.SetMulti(ctx, kvs)
	}
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error

	// Double write data to both caches
	eg.Go(func() error {
		srcErr = conf.src.SetMulti(gctx, kvs)
		return srcErr
	})

	eg.Go(func() error {
		dstErr = conf.dest.SetMulti(gctx, kvs)
		return nil // don't fail if there's an error from this cache
	})

	if err := eg.Wait(); err != nil {
		if dstErr == nil {
			// If error during write to source cache (source of truth), must delete from destination cache
			deleteMulti(ctx, conf.dest, kvs)
		}
		return err
	}

	if dstErr != nil {
		log.CtxWarningf(ctx, "Migration dest SetMulti of %v err: %s", kvs, dstErr)
	}

	return srcErr
}

func deleteMulti(ctx context.Context, dest interfaces.Cache, kvs map[*rspb.ResourceName][]byte) {
	eg, gctx := errgroup.WithContext(ctx)
	for r := range kvs {
		r := r
		eg.Go(func() error {
			deleteErr := dest.Delete(gctx, r)
			if deleteErr != nil && !status.IsNotFoundError(deleteErr) {
				log.CtxWarningf(ctx, "Migration double write err: src write of digest %v failed, but could not delete from dest cache: %s", r.GetDigest(), deleteErr)
			}
			return nil
		})
	}
	eg.Wait()
}

func (mc *MigrationCache) Delete(ctx context.Context, r *rspb.ResourceName) error {
	conf, err := mc.config(ctx)
	if err != nil {
		return err
	}
	if conf.dest == nil {
		return conf.src.Delete(ctx, r)
	}
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error

	eg.Go(func() error {
		srcErr = conf.src.Delete(gctx, r)
		return srcErr
	})

	eg.Go(func() error {
		dstErr = conf.dest.Delete(gctx, r)
		return nil // don't fail if there's an error from this cache
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	if dstErr != nil && (mc.logNotFoundErrors || !status.IsNotFoundError(dstErr)) {
		log.CtxWarningf(ctx, "Migration could not delete %v from dest cache: %s", r.GetDigest(), dstErr)
	}

	return srcErr
}

type doubleReader struct {
	ctx  context.Context
	src  io.ReadCloser
	dest io.ReadCloser

	r             *rspb.ResourceName
	bytesReadSrc  int
	bytesReadDest int
	lastSrcErr    error
	lastDestErr   error

	done chan struct{}
	// Pipe Writer and Reader used to read from decompressor
	pw               *io.PipeWriter
	pr               *io.PipeReader
	decompressor     io.WriteCloser
	mu               sync.Mutex // protects decompressSrcErr
	decompressSrcErr error
}

func (d *doubleReader) shouldVerifyNumBytes() bool {
	// Don't compare when the double read is off.
	if d.dest == nil {
		return false
	}

	// Don't compare byte differences for AC records, because there could be minor differences in metadata like timestamps
	if d.r.GetCacheType() != rspb.CacheType_CAS {
		return false
	}

	// If there are errors from the source reader, the number of bytes read from source and destination reader will be different.
	if d.lastSrcErr != io.EOF {
		return false
	}

	// If the destination reader failed, don't verify the number of bytes read.
	if d.lastDestErr != nil && d.lastDestErr != io.EOF {
		return false
	}

	if d.r.GetCompressor() != repb.Compressor_ZSTD {
		return true
	}

	// If compressed data is requested, we only verify some percent of the results.
	return d.decompressor != nil && d.decompressSrcErr == nil
}

func (d *doubleReader) Read(p []byte) (n int, err error) {
	var dstErr error
	eg := &errgroup.Group{}

	if d.dest != nil {
		eg.Go(func() error {
			var dstN int64
			dstN, dstErr = io.CopyN(io.Discard, d.dest, int64(len(p)))
			d.bytesReadDest += int(dstN)
			d.lastDestErr = dstErr
			return nil
		})
	}

	srcN, srcErr := d.src.Read(p)
	d.lastSrcErr = srcErr
	if d.decompressor != nil {
		eg.Go(func() error {
			_, err = d.decompressor.Write(p[:srcN])

			if err != nil {
				log.CtxWarningf(d.ctx, "Migration unable to decompress for resource %v: %s", d.r, err)
				d.mu.Lock()
				defer d.mu.Unlock()
				d.decompressSrcErr = err
				return nil
			}
			return nil
		})
	} else {
		d.bytesReadSrc += srcN
	}

	eg.Wait()
	// Don't log on EOF errors when reading chunks, because the readers from different caches
	// could be reading at different rates. Only log on Close() if the total number of bytes read differs
	if dstErr != nil && dstErr != srcErr && dstErr != io.EOF {
		log.CtxWarningf(d.ctx, "Migration %v read err, src err: %v, dest err: %s", d.r, srcErr, dstErr)
	}

	return srcN, srcErr
}

func (d *doubleReader) Close() error {
	eg := &errgroup.Group{}
	if d.dest != nil {
		if d.decompressor != nil {
			eg.Go(func() error {
				d.pw.Close()
				decompressErr := d.decompressor.Close()
				if decompressErr != nil {
					log.CtxWarningf(d.ctx, "Migration decompressor close err: %s", decompressErr)
					d.mu.Lock()
					defer d.mu.Unlock()
					d.decompressSrcErr = decompressErr
				}
				return nil
			})
		}

		eg.Go(func() error {
			if d.lastSrcErr == io.EOF && d.lastDestErr != io.EOF {
				n, err := io.Copy(io.Discard, d.dest)
				if err != nil {
					log.CtxWarningf(d.ctx, "Migration %v read err: failed to read remaining bytes from dest cache: %s", d.r, err)
					d.lastDestErr = err
				}
				d.bytesReadDest += int(n)
			}

			dstErr := d.dest.Close()
			if dstErr != nil {
				log.CtxWarningf(d.ctx, "Migration dest reader close err: %s", dstErr)
			}
			return nil
		})
	}

	srcErr := d.src.Close()
	eg.Wait()
	// Wait till we finish reading from the decompressor
	if d.decompressor != nil {
		<-d.done
	}
	if d.shouldVerifyNumBytes() && d.bytesReadDest != d.bytesReadSrc {
		log.CtxWarningf(d.ctx, "Migration %v read err, src read %d bytes, dest read %d bytes", d.r, d.bytesReadSrc, d.bytesReadDest)
	}

	return srcErr
}

func (mc *MigrationCache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	conf, err := mc.config(ctx)
	if err != nil {
		return nil, err
	}
	srcReader, srcErr := conf.src.Reader(ctx, r, uncompressedOffset, limit)
	if srcErr != nil || conf.dest == nil {
		return srcReader, srcErr
	}
	if !conf.doubleRead() {
		// We still want to copy if the source was successful.
		mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/, conf)
		return srcReader, srcErr
	}

	destReader, dstErr := conf.dest.Reader(ctx, r, uncompressedOffset, limit)
	if dstErr != nil {
		if mc.logNotFoundErrors || !status.IsNotFoundError(dstErr) {
			log.CtxWarningf(ctx, "Migration failed to get dest reader for %v: %s", r, dstErr)
		}
		if status.IsNotFoundError(dstErr) {
			mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/, conf)
			metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{
				metrics.CacheRequestType: "reader",
				metrics.GroupID:          groupID(ctx)}).Inc()
		} else {
			mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/, conf)
		}
		return srcReader, srcErr
	}
	metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{
		metrics.CacheRequestType: "reader",
		metrics.GroupID:          groupID(ctx),
	}).Inc()

	var decompressor io.WriteCloser
	pr, pw := io.Pipe()
	shouldDecompressAndVerify := r.GetCompressor() == repb.Compressor_ZSTD && conf.decompressRead()
	if shouldDecompressAndVerify {
		dr, err := compression.NewZstdDecompressingReader(destReader)
		if err != nil {
			log.CtxWarningf(ctx, "Migration failed to get dest decompressing reader for %v: %s", r, err)
		} else {
			destReader = dr
		}
		decompressor, err = compression.NewZstdDecompressor(pw)
		if err != nil {
			log.CtxWarningf(ctx, "Migration failed to get source decompressor for %v: %s", r, err)
		}
	}

	dr := &doubleReader{
		ctx:          ctx,
		src:          srcReader,
		dest:         destReader,
		r:            r,
		decompressor: decompressor,
		pw:           pw,
		pr:           pr,
		mu:           sync.Mutex{},
		done:         make(chan struct{}),
	}

	if shouldDecompressAndVerify {
		// Launch a go routine to read from the contents written to the
		// decompressor.
		go func() {
			defer close(dr.done)
			srcN, err := io.Copy(io.Discard, pr)
			if err != nil {
				log.Warningf("Migration failed to read from decompressor: %s", err)
				dr.mu.Lock()
				dr.decompressSrcErr = err
				dr.mu.Unlock()
			} else {
				dr.bytesReadSrc += int(srcN)
			}
		}()
	}

	return dr, nil
}

type doubleWriter struct {
	ctx          context.Context
	src          interfaces.CommittedWriteCloser
	dest         interfaces.CommittedWriteCloser
	wg           sync.WaitGroup
	destWriteErr error
}

func (d *doubleWriter) Write(data []byte) (int, error) {
	if d.destWriteErr == nil {
		d.wg.Add(1)
		defer d.wg.Wait()
		go func() {
			defer d.wg.Done()
			_, d.destWriteErr = d.dest.Write(data)
		}()
	}
	return d.src.Write(data)
}

func (d *doubleWriter) Commit() error {
	if d.destWriteErr != nil {
		log.CtxWarningf(d.ctx, "Migration destination writer not committing because of write error: %s", d.destWriteErr)
	} else {
		d.wg.Add(1)
		defer d.wg.Wait()
		go func() {
			defer d.wg.Done()
			if err := d.dest.Commit(); err != nil {
				log.CtxWarningf(d.ctx, "Migration destination writer commit err: %s", err)
			}
		}()
	}
	return d.src.Commit()
}

func (d *doubleWriter) Close() error {
	d.wg.Add(1)
	defer d.wg.Wait()
	go func() {
		defer d.wg.Done()
		if err := d.dest.Close(); err != nil {
			log.CtxWarningf(d.ctx, "Migration destination writer close err: %s", err)
		}
	}()
	return d.src.Close()
}

func (mc *MigrationCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	conf, err := mc.config(ctx)
	if err != nil {
		return nil, err
	}
	if conf.dest == nil {
		return conf.src.Writer(ctx, r)
	}
	if conf.asyncDestWrites {
		// We will write to the destination cache in the background.
		mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/, conf)
		return conf.src.Writer(ctx, r)
	}

	srcWriter, srcErr := conf.src.Writer(ctx, r)
	if srcErr != nil {
		return nil, srcErr
	}
	destWriter, dstErr := conf.dest.Writer(ctx, r)
	if dstErr != nil {
		log.CtxWarningf(ctx, "Migration failure creating dest %v writer: %s", r, dstErr)
		return srcWriter, nil
	}
	return &doubleWriter{
		ctx:  ctx,
		src:  srcWriter,
		dest: destWriter,
	}, nil
}

func (mc *MigrationCache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	conf, err := mc.config(ctx)
	if err != nil {
		return nil, err
	}
	srcBuf, srcErr := conf.src.Get(ctx, r)
	if conf.dest == nil || srcErr != nil {
		return srcBuf, srcErr
	}

	go func() {
		// Timeout is slightly larger than p99.9 latency.
		ctx, cancel := background.ExtendContextForFinalization(ctx, 5*time.Second)
		defer cancel()
		// Double read some proportion to guarantee that data is consistent between caches
		doubleRead := conf.doubleRead()
		if doubleRead {
			_, dstErr := conf.dest.Get(ctx, r)
			if dstErr != nil {
				if status.IsNotFoundError(dstErr) {
					metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{
						metrics.CacheRequestType: "get",
						metrics.GroupID:          groupID(ctx),
					}).Inc()
					if mc.logNotFoundErrors {
						log.CtxWarningf(ctx, "Migration dest read of %q not found", r)
					}
					mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/, conf)
				} else {
					log.CtxWarningf(ctx, "Double read of %q failed. src err %s, dest err %s", r, srcErr, dstErr)
					mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/, conf)
				}
			} else {
				metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{
					metrics.CacheRequestType: "get",
					metrics.GroupID:          groupID(ctx),
				}).Inc()
			}
		} else {
			mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/, conf)
		}
	}()

	// Return data from source cache
	return srcBuf, srcErr
}

func (mc *MigrationCache) sendNonBlockingCopy(ctx context.Context, r *rspb.ResourceName, onlyCopyMissing bool, conf *config) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	if onlyCopyMissing {
		alreadyCopied, err := conf.dest.Contains(ctx, r)
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
	select {
	case mc.copyChan <- &copyData{
		d:    r,
		ctx:  ctx,
		conf: conf,
	}:
	default:
		log.Debugf("Migration dropping copy digest %v, instance %s, cache %v", r.GetDigest(), r.GetInstanceName(), r.GetCacheType())
		mc.numCopiesDropped.Add(1)
	}
}

func (mc *MigrationCache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	wc, err := mc.Writer(ctx, r)
	if err != nil {
		return err
	}
	defer wc.Close()
	if _, err := wc.Write(data); err != nil {
		return err
	}
	return wc.Commit()
}

type copyData struct {
	d   *rspb.ResourceName
	ctx context.Context
	// Configuration for the request that triggered the copy
	conf *config
}

func (mc *MigrationCache) copyDataInBackground() error {
	rateLimiter := rate.NewLimiter(rate.Limit(mc.maxCopiesPerSec), 1)
	eg := &errgroup.Group{}
	for i := 0; i < mc.numCopyWorkers; i++ {
		eg.Go(func() error {
			for {
				if err := rateLimiter.Wait(context.Background()); err != nil {
					return err
				}
				select {
				case <-mc.quitChan:
					return nil
				case c := <-mc.copyChan:
					mc.copy(c)
				}
			}
		})
	}
	eg.Wait()

	// Drain copy channel on shutdown
	// We cannot close the channel before draining because there's a chance some cache requests may be
	// concurrently queued as the cache is shutting down, and if we try to enqueue a copy to the closed
	// channel it will panic
	for len(mc.copyChan) > 0 {
		<-mc.copyChan
	}
	return nil
}

func (mc *MigrationCache) monitorCopyChanFullness() {
	copyChanFullTicker := time.NewTicker(mc.copyChanFullWarningInterval)
	defer copyChanFullTicker.Stop()

	metricTicker := time.NewTicker(5 * time.Second)
	defer metricTicker.Stop()

	for {
		select {
		case <-mc.quitChan:
			return
		case <-copyChanFullTicker.C:
			dropped := mc.numCopiesDropped.Swap(0)
			if dropped != 0 {
				log.Warningf("Migration copy chan was full and dropped %d copies in %v. May need to increase buffer size", dropped, mc.copyChanFullWarningInterval)
			}
		case <-metricTicker.C:
			copyChanSize := len(mc.copyChan)
			metrics.MigrationCopyChanSize.Set(float64(copyChanSize))
		}
	}
}

func (mc *MigrationCache) copy(c *copyData) {
	ctx, cancel := background.ExtendContextForFinalization(c.ctx, 30*time.Second)
	c.ctx = ctx
	defer cancel()

	destContains, err := c.conf.dest.Contains(c.ctx, c.d)
	if err != nil {
		log.CtxWarningf(ctx, "Migration copy err: Could not call contains on dest cache %v: %s", c.d, err)
		return
	}
	if destContains {
		log.CtxDebugf(ctx, "Migration already copied on dequeue, returning early: digest %v", c.d)
		return
	}
	if c.d.GetDigest().GetSizeBytes() >= *compression.MinBytesAutoZstdCompression && c.conf.src.SupportsCompressor(repb.Compressor_ZSTD) && c.conf.dest.SupportsCompressor(repb.Compressor_ZSTD) {
		// Use compression if both caches support it. This will usually mean
		// that at src cache doesn't need to decompress and the dest cache
		// doesn't need to compress, which saves CPU and speeds up the copy.
		// Ideally, we would only do this when the destination cache would
		// automatically compress, but since there's no way to check that, check
		// the same flag that the destination cache would use.
		c.d = c.d.CloneVT()
		c.d.Compressor = repb.Compressor_ZSTD
	}

	srcReader, err := c.conf.src.Reader(c.ctx, c.d, 0, 0)
	if err != nil {
		if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Migration copy err: Could not create %v reader from src cache: %s", c.d, err)
		}
		return
	}
	defer srcReader.Close()

	destWriter, err := c.conf.dest.Writer(c.ctx, c.d)
	if err != nil {
		log.CtxWarningf(ctx, "Migration copy err: Could not create %v writer for dest cache: %s", c.d, err)
		return
	}
	defer destWriter.Close()

	n, err := io.Copy(destWriter, srcReader)
	if err != nil {
		log.CtxWarningf(ctx, "Migration copy err: Could not copy %v to dest cache: %s", c.d, err)
		return
	}

	if err := destWriter.Commit(); err != nil {
		log.CtxWarningf(ctx, "Migration copy err: destination commit failed: %s", err)
		return
	}

	labels := prometheus.Labels{
		metrics.CacheTypeLabel: cacheTypeLabel(c.d.GetCacheType()),
		metrics.GroupID:        groupID(ctx),
	}
	metrics.MigrationBlobsCopied.With(labels).Inc()
	metrics.MigrationBytesCopied.With(labels).Add(float64(n))
	log.CtxDebugf(ctx, "Migration successfully copied to dest cache: digest %v", c.d)
}

func cacheTypeLabel(ct rspb.CacheType) string {
	switch ct {
	case rspb.CacheType_AC:
		return "action"
	default:
		return "cas"
	}
}

func (mc *MigrationCache) Start() error {
	mc.quitChan = make(chan struct{})
	mc.eg.Go(func() error {
		mc.copyDataInBackground()
		return nil
	})
	if mc.copyChanFullWarningInterval > 0 {
		mc.eg.Go(func() error {
			mc.monitorCopyChanFullness()
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
	if src, canStopSrc := mc.defaultConfigDoNotUseDirectly.src.(interfaces.StoppableCache); canStopSrc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			srcShutdownErr = src.Stop()
			if srcShutdownErr != nil {
				log.Warningf("Migration src cache shutdown err: %s", srcShutdownErr)
			}
		}()
	}

	if dest, canStopDest := mc.defaultConfigDoNotUseDirectly.dest.(interfaces.StoppableCache); canStopDest {
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

// Compression should only be enabled during a migration if both the source and destination caches support the
// compressor, in order to reduce complexity around trying to double read/write compressed bytes to one cache and
// decompressed bytes to another
func (mc *MigrationCache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	return mc.defaultConfigDoNotUseDirectly.src.SupportsCompressor(compressor) &&
		mc.defaultConfigDoNotUseDirectly.dest.SupportsCompressor(compressor)
}
