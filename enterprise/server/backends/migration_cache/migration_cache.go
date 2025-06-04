package migration_cache

import (
	"context"
	"io"
	"math/rand"
	"slices"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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
	env                  environment.Env
	src                  interfaces.Cache
	dest                 interfaces.Cache
	doubleReadPercentage float64
	decompressPercentage float64
	logNotFoundErrors    bool

	eg       *errgroup.Group
	quitChan chan struct{}

	maxCopiesPerSec             int
	numCopyWorkers              int
	copyChan                    chan *copyData
	copyChanFullWarningInterval time.Duration
	numCopiesDropped            *atomic.Int64
	asyncDestWrites             bool
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
		env:                         env,
		src:                         srcCache,
		dest:                        destCache,
		doubleReadPercentage:        migrationConfig.DoubleReadPercentage,
		decompressPercentage:        migrationConfig.DecompressPercentage,
		logNotFoundErrors:           migrationConfig.LogNotFoundErrors,
		copyChan:                    make(chan *copyData, migrationConfig.CopyChanBufferSize),
		maxCopiesPerSec:             migrationConfig.MaxCopiesPerSec,
		eg:                          &errgroup.Group{},
		copyChanFullWarningInterval: time.Duration(migrationConfig.CopyChanFullWarningIntervalMin) * time.Minute,
		numCopiesDropped:            &atomic.Int64{},
		numCopyWorkers:              migrationConfig.NumCopyWorkers,
		asyncDestWrites:             migrationConfig.AsyncDestWrites,
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

func (mc *MigrationCache) doubleRead() bool {
	return mc.doubleReadPercentage > 0 && rand.Float64() < mc.doubleReadPercentage
}

func (mc *MigrationCache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	srcContains, srcErr := mc.src.Contains(ctx, r)

	if mc.doubleRead() {
		go func() {
			// Timeout is slightly larger than p99.9 latency.
			ctx, cancel := background.ExtendContextForFinalization(ctx, 2*time.Second)
			defer cancel()
			dstContains, dstErr := mc.dest.Contains(ctx, r)
			if dstErr != nil {
				log.Warningf("Migration dest %v contains failed: %s", r.GetDigest(), dstErr)
			} else if srcContains && !dstContains {
				metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "contains"}).Inc()
				if mc.logNotFoundErrors {
					log.Warningf("Migration digest %v src contains, dest does not", r.GetDigest())
				}
			} else if srcContains && dstContains {
				metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{metrics.CacheRequestType: "contains"}).Inc()
			}
		}()
	}

	return srcContains, srcErr
}

func (mc *MigrationCache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error
	var srcMetadata *interfaces.CacheMetadata

	eg.Go(func() error {
		srcMetadata, srcErr = mc.src.Metadata(gctx, r)
		return srcErr
	})

	doubleRead := mc.doubleRead()
	if doubleRead {
		eg.Go(func() error {
			_, dstErr = mc.dest.Metadata(gctx, r)
			return nil // we don't care about the return error from this cache
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if doubleRead {
		if dstErr != nil {
			if status.IsNotFoundError(dstErr) {
				metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "metadata"}).Inc()
			}
			if mc.logNotFoundErrors || !status.IsNotFoundError(dstErr) {
				log.Warningf("Migration dest %v metadata failed: %s", r.GetDigest(), dstErr)
			}
		} else {
			metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{metrics.CacheRequestType: "metadata"}).Inc()
		}
	}

	return srcMetadata, srcErr
}

func (mc *MigrationCache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	srcMissing, srcErr := mc.src.FindMissing(ctx, resources)

	if mc.doubleRead() {
		go func() {
			// Timeout is slightly larger than p99.9 latency.
			ctx, cancel := background.ExtendContextForFinalization(ctx, 2*time.Second)
			defer cancel()
			dstMissing, dstErr := mc.dest.FindMissing(ctx, resources)
			if dstErr != nil {
				log.Warningf("Migration dest FindMissing %v failed: %s", resources, dstErr)
			}
			missingOnlyInDest, _ := digest.Diff(srcMissing, dstMissing)
			if len(missingOnlyInDest) == 0 {
				metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{metrics.CacheRequestType: "findMissing"}).Inc()
			} else if len(missingOnlyInDest) > 0 {
				metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "findMissing"}).Inc()
				if mc.logNotFoundErrors {
					log.Warningf("Migration FindMissing diff for digests %v: src %v, dest %v", resources, srcMissing, dstMissing)
				}
				missingSet := make(map[string]struct{}, len(missingOnlyInDest))
				for _, d := range missingOnlyInDest {
					missingSet[d.GetHash()] = struct{}{}
				}
				for _, r := range resources {
					if _, missing := missingSet[r.GetDigest().GetHash()]; missing {
						mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/)
					}
				}
			}
		}()
	}
	return srcMissing, srcErr
}

func (mc *MigrationCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	srcData, srcErr := mc.src.GetMulti(ctx, resources)

	go func() {
		// Timeout is slightly larger than p99.9 latency.
		ctx, cancel := background.ExtendContextForFinalization(ctx, 10*time.Second)
		defer cancel()
		doubleRead := mc.doubleRead()
		var dstErr error
		var dstData map[*repb.Digest][]byte
		if doubleRead {
			dstData, dstErr = mc.dest.GetMulti(ctx, resources)
			if dstErr != nil {
				if mc.logNotFoundErrors || !status.IsNotFoundError(dstErr) {
					log.Warningf("Migration dest GetMulti of %v failed: %s", resources, dstErr)
				}
				if status.IsNotFoundError(dstErr) {
					metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "getMulti"}).Inc()
				}
			} else {
				metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{metrics.CacheRequestType: "getMulti"}).Inc()
			}
		}
		if doubleRead && dstErr == nil {
			for _, r := range resources {
				if _, inDest := dstData[r.GetDigest()]; !inDest {
					if srcErr != nil {
						mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/)
					} else if _, inSrc := srcData[r.GetDigest()]; inSrc {
						mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/)
					}
				}
			}
		} else {
			for _, r := range resources {
				mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/)
			}
		}
	}()
	// Return data from source cache
	return srcData, srcErr
}

func (mc *MigrationCache) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
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

func (mc *MigrationCache) deleteMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) {
	eg, gctx := errgroup.WithContext(ctx)
	for r := range kvs {
		r := r
		eg.Go(func() error {
			deleteErr := mc.dest.Delete(gctx, r)
			if deleteErr != nil && !status.IsNotFoundError(deleteErr) {
				log.Warningf("Migration double write err: src write of digest %v failed, but could not delete from dest cache: %s", r.GetDigest(), deleteErr)
			}
			return nil
		})
	}
	eg.Wait()
}

func (mc *MigrationCache) Delete(ctx context.Context, r *rspb.ResourceName) error {
	eg, gctx := errgroup.WithContext(ctx)
	var srcErr, dstErr error

	eg.Go(func() error {
		srcErr = mc.src.Delete(gctx, r)
		return srcErr
	})

	eg.Go(func() error {
		dstErr = mc.dest.Delete(gctx, r)
		return nil // don't fail if there's an error from this cache
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	if dstErr != nil && (mc.logNotFoundErrors || !status.IsNotFoundError(dstErr)) {
		log.Warningf("Migration could not delete %v from dest cache: %s", r.GetDigest(), dstErr)
	}

	return srcErr
}

type doubleReader struct {
	src  io.ReadCloser
	dest io.ReadCloser

	r             *rspb.ResourceName
	doubleReadBuf []byte
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
			// Grow never changes the length, so it will stay 0.
			d.doubleReadBuf = slices.Grow(d.doubleReadBuf, len(p))

			var dstN int
			dstN, dstErr = io.ReadFull(d.dest, d.doubleReadBuf[:len(p)])
			if dstErr == io.ErrUnexpectedEOF {
				dstErr = io.EOF
			}
			d.bytesReadDest += dstN
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
				log.Warningf("Migration unable to decompress for digest %q: %s", d.r.GetDigest().GetHash(), err)
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
		log.Warningf("Migration %v read err, src err: %v, dest err: %s", d.r, srcErr, dstErr)
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
					log.Warningf("Migration decompressor close err: %s", decompressErr)
					d.mu.Lock()
					defer d.mu.Unlock()
					d.decompressSrcErr = decompressErr
				}
				return nil
			})
		}

		eg.Go(func() error {
			if d.lastSrcErr == io.EOF && d.lastDestErr != io.EOF {
				destBuf, err := io.ReadAll(d.dest)
				if err != nil {
					log.Warningf("Migration %v read err: failed to read remaining bytes from dest cache: %s", d.r, err)
				}
				d.bytesReadDest += len(destBuf)
			}

			dstErr := d.dest.Close()
			if dstErr != nil {
				log.Warningf("Migration dest reader close err: %s", dstErr)
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
		log.Warningf("Migration %v read err, src read %d bytes, dest read %d bytes", d.r, d.bytesReadSrc, d.bytesReadDest)
	}

	return srcErr
}

func (mc *MigrationCache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	eg := &errgroup.Group{}
	var dstErr error
	var destReader io.ReadCloser
	var decompressor io.WriteCloser
	pr, pw := io.Pipe()

	doubleRead := mc.doubleRead()

	shouldDecompressAndVerify := false
	if doubleRead && r.GetCompressor() == repb.Compressor_ZSTD {
		shouldDecompressAndVerify = (rand.Float64() <= mc.decompressPercentage)
	}

	if doubleRead {
		eg.Go(func() error {
			destReader, dstErr = mc.dest.Reader(ctx, r, uncompressedOffset, limit)
			if dstErr != nil {
				shouldDecompressAndVerify = false
				return nil
			}
			metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{metrics.CacheRequestType: "reader"}).Inc()
			if shouldDecompressAndVerify {
				dr, err := compression.NewZstdDecompressingReader(destReader)
				if err != nil {
					log.Warningf("Migration failed to get dest decompressing reader for digest %q: %s", r.GetDigest().GetHash(), err)
				} else {
					destReader = dr
				}
				decompressor, err = compression.NewZstdDecompressor(pw)
				if err != nil {
					log.Warningf("Migration failed to get source decompressor for digest %q: %s", r.GetDigest().GetHash(), err)
				}
			}
			return nil
		})
	}

	srcReader, srcErr := mc.src.Reader(ctx, r, uncompressedOffset, limit)
	eg.Wait()

	bothCacheNotFound := status.IsNotFoundError(srcErr) && status.IsNotFoundError(dstErr)
	shouldLogErr := mc.logNotFoundErrors || !status.IsNotFoundError(dstErr)
	if dstErr != nil && !bothCacheNotFound {
		if status.IsNotFoundError(dstErr) {
			metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "reader"}).Inc()
		}
		if shouldLogErr {
			log.Warningf("%v reader failed for dest cache: %s", r.GetDigest(), dstErr)
		}
	}

	if srcErr != nil {
		if destReader != nil {
			err := destReader.Close()
			if err != nil {
				log.Warningf("Migration dest reader close err: %s", err)
			}
		}
		log.Debugf("Migration %v src reader err, doubleRead is %v: %s", r.GetDigest(), doubleRead, srcErr)
		return nil, srcErr
	}

	mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/)

	dr := &doubleReader{
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
	src  interfaces.CommittedWriteCloser
	dest *asyncWriter
}

func (d *doubleWriter) Write(data []byte) (int, error) {
	d.dest.Write(data)
	return d.src.Write(data)
}

func (d *doubleWriter) Commit() error {
	d.dest.Commit()
	return d.src.Commit()
}

func (d *doubleWriter) Close() error {
	err := d.src.Close()
	// The destination writer might be closing in the background, so close it
	// second to give it time to finish.
	d.dest.Close()
	return err
}

// asyncWriter is an interfaces.CommittedWriteCloser which doesn't block on
// Write and Commit calls. Upon Close, it blocks until all pending writes and a
// possible commit have completed. Its methods never return errors, so it's
// only suitable for use when writing to a destination cache.
type asyncWriter struct {
	ops      chan asyncOp
	done     chan struct{}
	finished bool
}

type asyncOp struct {
	data          []byte
	commit, close bool
}

func newAsyncWriter(cwc interfaces.CommittedWriteCloser) *asyncWriter {
	aw := &asyncWriter{
		// Small buffer to avoid total stalls
		ops:  make(chan asyncOp, 10),
		done: make(chan struct{}),
	}
	go aw.run(cwc)
	return aw
}

func (aw *asyncWriter) run(cwc interfaces.CommittedWriteCloser) {
	defer close(aw.done)
	var writeErr error
	for op := range aw.ops {
		if op.commit {
			if writeErr == nil {
				if err := cwc.Commit(); err != nil {
					log.Warningf("Migration writer commit err: %s", err)
				}
			} else {
				log.Warningf("Migration writer not committing because of write error: %s", writeErr)
			}
		}
		if op.close || op.commit {
			// Close even on commits, because close always comes after commit.
			if err := cwc.Close(); err != nil {
				log.Warningf("Migration writer close err: %s", err)
			}
			return
		}
		if writeErr != nil {
			// ignore writes after an error
			continue
		}
		n, err := cwc.Write(op.data)
		if err != nil {
			writeErr = err
		}
		if n != len(op.data) {
			writeErr = io.ErrShortWrite
		}
	}
}

func (aw *asyncWriter) Write(data []byte) (int, error) {
	if aw.finished {
		log.Warning("asyncWriter attempting writes after commit or close")
		return 0, io.ErrClosedPipe
	}
	aw.ops <- asyncOp{data: data}
	return len(data), nil
}

func (aw *asyncWriter) finish(op asyncOp) {
	if !aw.finished {
		aw.finished = true
		aw.ops <- op
		close(aw.ops)
	}
}

func (aw *asyncWriter) Commit() error {
	aw.finish(asyncOp{commit: true})
	return nil
}

func (aw *asyncWriter) Close() error {
	aw.finish(asyncOp{close: true})
	// wait for all the ops to be done, so clients can read their writes
	<-aw.done
	return nil
}

func (mc *MigrationCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	if mc.asyncDestWrites {
		// We will write to the destination cache in the background.
		mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/)
		return mc.src.Writer(ctx, r)
	}

	srcWriter, srcErr := mc.src.Writer(ctx, r)
	if srcErr != nil {
		return nil, srcErr
	}
	destWriter, dstErr := mc.dest.Writer(ctx, r)
	if dstErr != nil {
		log.Warningf("Migration failure creating dest %v writer: %s", r.GetDigest(), dstErr)
		return srcWriter, nil
	}

	dw := &doubleWriter{
		src:  srcWriter,
		dest: newAsyncWriter(destWriter),
	}
	return dw, nil
}

func (mc *MigrationCache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	srcBuf, srcErr := mc.src.Get(ctx, r)

	go func() {
		// Timeout is slightly larger than p99.9 latency.
		ctx, cancel := background.ExtendContextForFinalization(ctx, 5*time.Second)
		defer cancel()
		// Double read some proportion to guarantee that data is consistent between caches
		doubleRead := mc.doubleRead()
		if doubleRead {
			_, dstErr := mc.dest.Get(ctx, r)
			if dstErr != nil {
				if status.IsNotFoundError(dstErr) {
					metrics.MigrationNotFoundErrorCount.With(prometheus.Labels{metrics.CacheRequestType: "get"}).Inc()
					mc.sendNonBlockingCopy(ctx, r, false /*=onlyCopyMissing*/)
				} else {
					if mc.logNotFoundErrors {
						log.Warningf("Double read of %q failed. src err %s, dest err %s", r, srcErr, dstErr)
					}
					mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/)
				}
			} else {
				metrics.MigrationDoubleReadHitCount.With(prometheus.Labels{metrics.CacheRequestType: "get"}).Inc()
			}
		} else {
			mc.sendNonBlockingCopy(ctx, r, true /*=onlyCopyMissing*/)
		}
	}()

	// Return data from source cache
	return srcBuf, srcErr
}

func (mc *MigrationCache) sendNonBlockingCopy(ctx context.Context, r *rspb.ResourceName, onlyCopyMissing bool) {
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
	select {
	case mc.copyChan <- &copyData{
		d:   r,
		ctx: ctx,
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
	ctx, cancel := background.ExtendContextForFinalization(c.ctx, 10*time.Second)
	c.ctx = ctx
	defer cancel()

	destContains, err := mc.dest.Contains(c.ctx, c.d)
	if err != nil {
		log.Warningf("Migration copy err: Could not call contains on dest cache %v: %s", c.d, err)
		return
	}
	if destContains {
		log.Debugf("Migration already copied on dequeue, returning early: digest %v", c.d)
		return
	}

	srcReader, err := mc.src.Reader(c.ctx, c.d, 0, 0)
	if err != nil {
		if !status.IsNotFoundError(err) {
			log.Warningf("Migration copy err: Could not create %v reader from src cache: %s", c.d, err)
		}
		return
	}
	defer srcReader.Close()

	destWriter, err := mc.dest.Writer(c.ctx, c.d)
	if err != nil {
		log.Warningf("Migration copy err: Could not create %v writer for dest cache: %s", c.d, err)
		return
	}
	defer destWriter.Close()

	n, err := io.Copy(destWriter, srcReader)
	if err != nil {
		log.Warningf("Migration copy err: Could not copy %v to dest cache: %s", c.d, err)
		return
	}

	if err := destWriter.Commit(); err != nil {
		log.Warningf("Migration copy err: destination commit failed: %s", err)
		return
	}

	ctLabel := cacheTypeLabel(c.d.GetCacheType())
	metrics.MigrationBlobsCopied.With(prometheus.Labels{metrics.CacheTypeLabel: ctLabel}).Inc()
	metrics.MigrationBytesCopied.With(prometheus.Labels{metrics.CacheTypeLabel: ctLabel}).Add(float64(n))
	log.Debugf("Migration successfully copied to dest cache: digest %v", c.d)
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

// Compression should only be enabled during a migration if both the source and destination caches support the
// compressor, in order to reduce complexity around trying to double read/write compressed bytes to one cache and
// decompressed bytes to another
func (mc *MigrationCache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	return mc.src.SupportsCompressor(compressor) && mc.dest.SupportsCompressor(compressor)
}
