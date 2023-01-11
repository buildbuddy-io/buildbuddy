package pebble_cache

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebbleutil"
	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/docker/go-units"
	"github.com/elastic/gosigar"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

var (
	nameFlag                   = flag.String("cache.pebble.name", DefaultName, "The name used in reporting cache metrics and status.")
	rootDirectoryFlag          = flag.String("cache.pebble.root_directory", "", "The root directory to store the database in.")
	blockCacheSizeBytesFlag    = flag.Int64("cache.pebble.block_cache_size_bytes", DefaultBlockCacheSizeBytes, "How much ram to give the block cache")
	maxInlineFileSizeBytesFlag = flag.Int64("cache.pebble.max_inline_file_size_bytes", DefaultMaxInlineFileSizeBytes, "Files smaller than this may be inlined directly into pebble")
	partitionsFlag             = flagutil.New("cache.pebble.partitions", []disk.Partition{}, "")
	partitionMappingsFlag      = flagutil.New("cache.pebble.partition_mappings", []disk.PartitionMapping{}, "")

	// TODO(tylerw): remove most of these flags post-migration.
	migrateFromDiskDir        = flag.String("cache.pebble.migrate_from_disk_dir", "", "If set, attempt to migrate this disk dir to a new pebble cache")
	forceAllowMigration       = flag.Bool("cache.pebble.force_allow_migration", false, "If set, allow migrating into an existing pebble cache")
	clearCacheBeforeMigration = flag.Bool("cache.pebble.clear_cache_before_migration", false, "If set, clear any existing cache content before migrating")
	scanForOrphanedFiles      = flag.Bool("cache.pebble.scan_for_orphaned_files", false, "If true, scan for orphaned files")
	orphanDeleteDryRun        = flag.Bool("cache.pebble.orphan_delete_dry_run", true, "If set, log orphaned files instead of deleting them")
	dirDeletionDelay          = flag.Duration("cache.pebble.dir_deletion_delay", time.Hour, "How old directories must be before being eligible for deletion when empty")
	atimeUpdateThresholdFlag  = flag.Duration("cache.pebble.atime_update_threshold", DefaultAtimeUpdateThreshold, "Don't update atime if it was updated more recently than this")
	atimeWriteBatchSizeFlag   = flag.Int("cache.pebble.atime_write_batch_size", DefaultAtimeWriteBatchSize, "Buffer this many writes before writing atime data")
	atimeBufferSizeFlag       = flag.Int("cache.pebble.atime_buffer_size", DefaultAtimeBufferSize, "Buffer up to this many atime updates in a channel before dropping atime updates")
	minEvictionAgeFlag        = flag.Duration("cache.pebble.min_eviction_age", DefaultMinEvictionAge, "Don't evict anything unless it's been idle for at least this long")
	forceCompaction           = flag.Bool("cache.pebble.force_compaction", false, "If set, compact the DB when it's created")
	forceCalculateMetadata    = flag.Bool("cache.pebble.force_calculate_metadata", false, "If set, partition size and counts will be calculated even if cached information is available.")
	isolateByGroupIDsFlag     = flag.Bool("cache.pebble.isolate_by_group_ids", false, "If set, filepaths and filekeys for AC records will include groupIDs")
	samplesPerEviction        = flag.Int("cache.pebble.samples_per_eviction", 20, "How many records to sample on each eviction")
	samplePoolSize            = flag.Int("cache.pebble.sample_pool_size", 500, "How many deletion candidates to maintain between evictions")

	// Compression related flags
	// TODO(Maggie): Remove enableZstdCompressionFlag after migration
	enableZstdCompressionFlag   = flag.Bool("cache.pebble.enable_zstd_compression", false, "If set, zstd compressed blobs can be saved to the cache. Otherwise only decompressed bytes are stored.")
	minBytesAutoZstdCompression = flag.Int64("cache.pebble.min_bytes_auto_zstd_compression", math.MaxInt64, "Blobs larger than this will be zstd compressed before written to disk.")

	// Default values for Options
	// (It is valid for these options to be 0, so we use ptrs to indicate whether they're set.
	// Their defaults must be vars so we can take their addresses)
	DefaultAtimeUpdateThreshold = 10 * time.Minute
	DefaultAtimeBufferSize      = 100000
	DefaultMinEvictionAge       = 6 * time.Hour

	DefaultName         = "pebble_cache"
	DefaultMaxSizeBytes = cache_config.MaxSizeBytes()

	// Prefix used to store non-record data.
	SystemKeyPrefix = []byte{'\x01'}

	acDir  = []byte("/ac/")
	casDir = []byte("/cas/")
)

const (
	// cutoffThreshold is the point above which a janitor thread will run
	// and delete the oldest items from the cache.
	JanitorCutoffThreshold = .90

	// janitorCheckPeriod is how often the janitor thread will wake up to
	// check the cache size.
	JanitorCheckPeriod = 1 * time.Second
	megabyte           = 1e6

	defaultPartitionID           = "default"
	partitionDirectoryPrefix     = "PT"
	partitionMetadataFlushPeriod = 5 * time.Second
	metricsRefreshPeriod         = 30 * time.Second

	// atimeFlushPeriod is the time interval that we will wait before
	// flushing any atime updates in an incomplete batch (that have not
	// already been flushed due to throughput)
	atimeFlushPeriod = 10 * time.Second

	// CompressorBufSizeBytes is the buffer size we use for each chunk when compressing data
	// It should be relatively large to get a good compression ratio bc each chunk is compressed independently
	CompressorBufSizeBytes = 4e6 // 4 MB

	// Default values for Options
	DefaultBlockCacheSizeBytes    = int64(1000 * megabyte)
	DefaultMaxInlineFileSizeBytes = int64(1024)
	DefaultAtimeWriteBatchSize    = 1000
)

// Options is a struct containing the pebble cache configuration options.
// Once a cache is created, the options may not be changed.
type Options struct {
	Name              string
	RootDirectory     string
	Partitions        []disk.Partition
	PartitionMappings []disk.PartitionMapping
	IsolateByGroupIDs bool

	EnableZstdCompression       bool
	MinBytesAutoZstdCompression int64

	MaxSizeBytes           int64
	BlockCacheSizeBytes    int64
	MaxInlineFileSizeBytes int64

	AtimeUpdateThreshold *time.Duration
	AtimeWriteBatchSize  int
	AtimeBufferSize      *int
	MinEvictionAge       *time.Duration
}

type sizeUpdate struct {
	partID string
	key    []byte
	delta  int64
}

type accessTimeUpdate struct {
	fileMetadataKey []byte
}

// PebbleCache implements the cache interface by storing metadata in a pebble
// database and storing cache entry contents on disk.
type PebbleCache struct {
	name              string
	rootDirectory     string
	partitions        []disk.Partition
	partitionMappings []disk.PartitionMapping

	maxSizeBytes           int64
	blockCacheSizeBytes    int64
	maxInlineFileSizeBytes int64

	atimeUpdateThreshold time.Duration
	atimeWriteBatchSize  int
	atimeBufferSize      int
	minEvictionAge       time.Duration

	env    environment.Env
	db     *pebble.DB
	leaser pebbleutil.Leaser

	edits    chan *sizeUpdate
	accesses chan *accessTimeUpdate

	quitChan      chan struct{}
	eg            *errgroup.Group
	egSizeUpdates *errgroup.Group

	statusMu *sync.Mutex // PROTECTS(evictors)
	evictors []*partitionEvictor

	brokenFilesDone   chan struct{}
	orphanedFilesDone chan struct{}

	fileStorer filestore.Store
	bufferPool *bytebufferpool.Pool

	enableZstdCompression       bool
	minBytesAutoZstdCompression int64

	// TODO(Maggie): Clean this up after the isolateByGroupIDs migration
	isolateByGroupIDs bool
}

// Register creates a new PebbleCache from the configured flags and sets it in
// the provided env.
func Register(env environment.Env) error {
	if *rootDirectoryFlag == "" {
		return nil
	}
	if *clearCacheBeforeMigration {
		if err := os.RemoveAll(*rootDirectoryFlag); err != nil {
			return err
		}
	}
	if err := disk.EnsureDirectoryExists(*rootDirectoryFlag); err != nil {
		return err
	}
	migrateDir := ""
	if *migrateFromDiskDir != "" {
		// Ensure a pebble DB doesn't already exist if we are migrating.
		// But allow anyway if forceAllowMigration was set.
		desc, err := pebble.Peek(*rootDirectoryFlag, vfs.Default)
		if err != nil {
			return err
		}
		if desc.Exists && !*forceAllowMigration {
			log.Warningf("Pebble DB at %q already exists, cannot migrate from disk dir: %q", *rootDirectoryFlag, *migrateFromDiskDir)
		} else {
			migrateDir = *migrateFromDiskDir
		}
	}
	opts := &Options{
		Name:                        *nameFlag,
		RootDirectory:               *rootDirectoryFlag,
		Partitions:                  *partitionsFlag,
		PartitionMappings:           *partitionMappingsFlag,
		IsolateByGroupIDs:           *isolateByGroupIDsFlag,
		EnableZstdCompression:       *enableZstdCompressionFlag,
		BlockCacheSizeBytes:         *blockCacheSizeBytesFlag,
		MaxSizeBytes:                cache_config.MaxSizeBytes(),
		MaxInlineFileSizeBytes:      *maxInlineFileSizeBytesFlag,
		MinBytesAutoZstdCompression: *minBytesAutoZstdCompression,
		AtimeUpdateThreshold:        atimeUpdateThresholdFlag,
		AtimeWriteBatchSize:         *atimeWriteBatchSizeFlag,
		AtimeBufferSize:             atimeBufferSizeFlag,
		MinEvictionAge:              minEvictionAgeFlag,
	}
	c, err := NewPebbleCache(env, opts)
	if err != nil {
		return status.InternalErrorf("Error configuring pebble cache: %s", err)
	}
	if *forceCompaction {
		log.Infof("Pebble Cache: starting manual compaction...")
		start := time.Now()
		err := c.db.Compact(keys.MinByte, keys.MaxByte, true /*=parallelize*/)
		log.Infof("Pebble Cache: manual compaction finished in %s", time.Since(start))
		if err != nil {
			log.Errorf("Error during compaction: %s", err)
		}
	}
	if migrateDir != "" {
		if err := c.MigrateFromDiskDir(migrateDir); err != nil {
			return err
		}
	}
	c.Start()
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return c.Stop()
	})

	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with pebble_cache.")
	}
	env.SetCache(c)
	return nil
}

// validateOpts validates that each partition mapping references a partition
// and that MaxSizeBytes is non-zero.
func validateOpts(opts *Options) error {
	if opts.RootDirectory == "" {
		return status.FailedPreconditionError("Pebble cache root directory must be set")
	}
	if opts.MaxSizeBytes == 0 {
		return status.FailedPreconditionError("Pebble cache size must be greater than 0")
	}

	for _, pm := range opts.PartitionMappings {
		found := false
		for _, p := range opts.Partitions {
			if p.ID == pm.PartitionID {
				found = true
				break
			}
		}
		if !found {
			return status.NotFoundErrorf("Mapping to unknown partition %q", pm.PartitionID)
		}
	}

	if opts.MinBytesAutoZstdCompression < opts.MaxInlineFileSizeBytes {
		return status.FailedPreconditionError("pebble cache should not compress inlined data because it is already compressed")
	}

	return nil
}

// SetOptionDefaults sets default values on Options if they are not set
func SetOptionDefaults(opts *Options) {
	if opts.Name == "" {
		opts.Name = DefaultName
	}
	if opts.MaxSizeBytes == 0 {
		opts.MaxSizeBytes = DefaultMaxSizeBytes
	}
	if opts.BlockCacheSizeBytes == 0 {
		opts.BlockCacheSizeBytes = DefaultBlockCacheSizeBytes
	}
	if opts.MaxInlineFileSizeBytes == 0 {
		opts.MaxInlineFileSizeBytes = DefaultMaxInlineFileSizeBytes
	}
	if opts.MinBytesAutoZstdCompression == 0 {
		opts.MinBytesAutoZstdCompression = DefaultMaxInlineFileSizeBytes
	}
	if opts.AtimeUpdateThreshold == nil {
		opts.AtimeUpdateThreshold = &DefaultAtimeUpdateThreshold
	}
	if opts.AtimeWriteBatchSize == 0 {
		opts.AtimeWriteBatchSize = DefaultAtimeWriteBatchSize
	}
	if opts.AtimeBufferSize == nil {
		opts.AtimeBufferSize = &DefaultAtimeBufferSize
	}
	if opts.MinEvictionAge == nil {
		opts.MinEvictionAge = &DefaultMinEvictionAge
	}
}

func ensureDefaultPartitionExists(opts *Options) {
	foundDefaultPartition := false
	for _, part := range opts.Partitions {
		if part.ID == defaultPartitionID {
			foundDefaultPartition = true
		}
	}
	if foundDefaultPartition {
		return
	}
	opts.Partitions = append(opts.Partitions, disk.Partition{
		ID:           defaultPartitionID,
		MaxSizeBytes: opts.MaxSizeBytes,
	})
}

// defaultPebbleOptions returns default pebble config options.
func defaultPebbleOptions() *pebble.Options {
	// TODO: tune options here.
	return &pebble.Options{}
}

// NewPebbleCache creates a new cache from the provided env and opts.
func NewPebbleCache(env environment.Env, opts *Options) (*PebbleCache, error) {
	SetOptionDefaults(opts)
	if err := validateOpts(opts); err != nil {
		return nil, err
	}
	if err := disk.EnsureDirectoryExists(opts.RootDirectory); err != nil {
		return nil, err
	}
	ensureDefaultPartitionExists(opts)

	pebbleOptions := defaultPebbleOptions()
	if opts.BlockCacheSizeBytes > 0 {
		c := pebble.NewCache(opts.BlockCacheSizeBytes)
		defer c.Unref()
		pebbleOptions.Cache = c
	}

	db, err := pebble.Open(opts.RootDirectory, pebbleOptions)
	if err != nil {
		return nil, err
	}
	pc := &PebbleCache{
		name:                        opts.Name,
		rootDirectory:               opts.RootDirectory,
		partitions:                  opts.Partitions,
		partitionMappings:           opts.PartitionMappings,
		maxSizeBytes:                opts.MaxSizeBytes,
		blockCacheSizeBytes:         opts.BlockCacheSizeBytes,
		maxInlineFileSizeBytes:      opts.MaxInlineFileSizeBytes,
		atimeUpdateThreshold:        *opts.AtimeUpdateThreshold,
		atimeWriteBatchSize:         opts.AtimeWriteBatchSize,
		atimeBufferSize:             *opts.AtimeBufferSize,
		minEvictionAge:              *opts.MinEvictionAge,
		env:                         env,
		db:                          db,
		leaser:                      pebbleutil.NewDBLeaser(db),
		brokenFilesDone:             make(chan struct{}),
		orphanedFilesDone:           make(chan struct{}),
		eg:                          &errgroup.Group{},
		egSizeUpdates:               &errgroup.Group{},
		statusMu:                    &sync.Mutex{},
		edits:                       make(chan *sizeUpdate, 1000),
		accesses:                    make(chan *accessTimeUpdate, *opts.AtimeBufferSize),
		evictors:                    make([]*partitionEvictor, len(opts.Partitions)),
		fileStorer:                  filestore.New(filestore.Opts{IsolateByGroupIDs: opts.IsolateByGroupIDs}),
		isolateByGroupIDs:           opts.IsolateByGroupIDs,
		bufferPool:                  bytebufferpool.New(CompressorBufSizeBytes),
		minBytesAutoZstdCompression: opts.MinBytesAutoZstdCompression,
		enableZstdCompression:       opts.EnableZstdCompression,
	}

	peMu := sync.Mutex{}
	eg := errgroup.Group{}
	for i, part := range opts.Partitions {
		i := i
		part := part
		eg.Go(func() error {
			blobDir := pc.blobDir(!opts.IsolateByGroupIDs, part.ID)
			if err := disk.EnsureDirectoryExists(blobDir); err != nil {
				return err
			}
			pe, err := newPartitionEvictor(part, pc.fileStorer, blobDir, pc.leaser, pc.accesses, *opts.AtimeBufferSize, *opts.MinEvictionAge, opts.Name, opts.IsolateByGroupIDs)
			if err != nil {
				return err
			}
			peMu.Lock()
			pc.evictors[i] = pe
			peMu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	statusz.AddSection(opts.Name, "On disk LRU cache", pc)
	return pc, nil
}

func keyPrefix(prefix, key []byte) []byte {
	v := make([]byte, 0, len(prefix)+len(key))
	v = append(v, prefix...)
	v = append(v, key...)
	return v
}

func keyRange(key []byte) ([]byte, []byte) {
	return keyPrefix(key, keys.MinByte), keyPrefix(key, keys.MaxByte)
}

func olderThanThreshold(t time.Time, threshold time.Duration) bool {
	age := time.Since(t)
	return age >= threshold
}

func (p *PebbleCache) batchEditAtime(batch *pebble.Batch, fileMetadataKey []byte, fileMetadata *rfpb.FileMetadata) error {
	atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
	if !olderThanThreshold(atime, p.atimeUpdateThreshold) {
		return nil
	}
	fileMetadata.LastAccessUsec = time.Now().UnixMicro()
	protoBytes, err := proto.Marshal(fileMetadata)
	if err != nil {
		return err
	}

	return batch.Set(fileMetadataKey, protoBytes, nil /*ignored write options*/)
}

func (p *PebbleCache) processAccessTimeUpdates(quitChan chan struct{}) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	batch := db.NewBatch()
	var lastWrite time.Time

	flush := func() error {
		if batch.Count() > 0 {
			if err := batch.Commit(&pebble.WriteOptions{Sync: true}); err != nil {
				return err
			}
			batch = db.NewBatch()
			lastWrite = time.Now()
		}
		return nil
	}

	exitMu := sync.Mutex{}
	exiting := false
	go func() {
		<-quitChan
		exitMu.Lock()
		exiting = true
		exitMu.Unlock()
	}()

	for {
		select {
		case accessTimeUpdate := <-p.accesses:
			md, err := readFileMetadata(db, accessTimeUpdate.fileMetadataKey)
			if err != nil {
				log.Warningf("Error unmarshaling data for %q: %s", accessTimeUpdate.fileMetadataKey, err)
				continue
			}
			if err := p.batchEditAtime(batch, accessTimeUpdate.fileMetadataKey, md); err != nil {
				log.Warningf("Error updating atime: %s", err)
			}
			if int(batch.Count()) >= p.atimeWriteBatchSize {
				flush()
			}
		case <-time.After(time.Second):
			if time.Since(lastWrite) > atimeFlushPeriod {
				flush()
			}

			exitMu.Lock()
			done := exiting
			exitMu.Unlock()

			if done {
				flush()
				return nil
			}
		}
	}
}

func (p *PebbleCache) processSizeUpdates() {
	evictors := make(map[string]*partitionEvictor, 0)
	p.statusMu.Lock()
	for _, pe := range p.evictors {
		evictors[pe.part.ID] = pe
	}
	p.statusMu.Unlock()

	for edit := range p.edits {
		e := evictors[edit.partID]
		e.updateSize(edit.key, edit.delta)
	}
}

func (p *PebbleCache) deleteOrphanedFiles(quitChan chan struct{}) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	const sep = "/"
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: keys.MinByte,
		UpperBound: keys.MaxByte,
	})
	defer iter.Close()

	orphanCount := 0
	for _, part := range p.partitions {
		walkFn := func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}

			// Check if we're shutting down; exit if so.
			select {
			case <-quitChan:
				return status.CanceledErrorf("cache shutting down")
			default:
			}

			dupPartitionPattern := fmt.Sprintf("%s/%s", part.ID, part.ID)
			pathHasDupPartitionID := strings.Contains(path, dupPartitionPattern)
			blobDir := p.blobDir(pathHasDupPartitionID, part.ID)

			relPath, err := filepath.Rel(blobDir, path)
			if err != nil {
				return err
			}
			parts := strings.Split(relPath, sep)
			if len(parts) < 3 {
				log.Warningf("Skipping orphaned file: %q", path)
				return nil
			}
			prefixIndex := len(parts) - 2
			// Remove the second to last element which is the 4-char hash prefix.
			parts = append(parts[:prefixIndex], parts[prefixIndex+1:]...)
			fileMetadataKey := []byte(strings.Join(parts, sep))
			if _, err := lookupFileMetadata(p.env.GetServerContext(), iter, fileMetadataKey); status.IsNotFoundError(err) {
				if *orphanDeleteDryRun {
					fi, err := d.Info()
					if err != nil {
						return err
					}
					log.Infof("Would delete orphaned file: %s (last modified: %s) which is not in cache", path, fi.ModTime())
				} else {
					if err := os.Remove(path); err == nil {
						log.Infof("Removed orphaned file: %q", path)
					}
				}
				orphanCount += 1
			}

			if orphanCount%1000 == 0 && orphanCount != 0 {
				log.Infof("Removed %d orphans", orphanCount)
			}
			return nil
		}
		blobDir := p.blobDir(!p.isolateByGroupIDs, part.ID)
		if err := filepath.WalkDir(blobDir, walkFn); err != nil {
			alert.UnexpectedEvent("pebble_cache_error_deleting_orphans", "err: %s", err)
		}
	}
	log.Infof("Pebble Cache: deleteOrphanedFiles removed %d files", orphanCount)
	close(p.orphanedFilesDone)
	return nil
}

func (p *PebbleCache) scanForBrokenFiles(quitChan chan struct{}) error {
	db, err := p.leaser.DB()
	if err != nil {
		return nil
	}
	defer db.Close()

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: keys.MinByte,
		UpperBound: keys.MaxByte,
	})
	defer iter.Close()

	fileMetadata := &rfpb.FileMetadata{}
	blobDir := ""

	defer func() {
		close(p.brokenFilesDone)
	}()

	mismatchCount := 0
	for iter.First(); iter.Valid(); iter.Next() {
		// Check if we're shutting down; exit if so.
		select {
		case <-quitChan:
			return nil
		default:
		}

		if bytes.HasPrefix(iter.Key(), SystemKeyPrefix) {
			continue
		}

		// Attempt a read -- if the file is unreadable; update the metadata.
		fileMetadataKey := iter.Key()
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			log.Errorf("Error unmarshaling metadata when scanning for broken files: %s", err)
			continue
		}
		pathHasDupPartitionID := !bytes.HasPrefix(fileMetadataKey, []byte("PT"))
		blobDir = p.blobDir(pathHasDupPartitionID, fileMetadata.GetFileRecord().GetIsolation().GetPartitionId())
		_, err := p.fileStorer.NewReader(p.env.GetServerContext(), blobDir, fileMetadata.GetStorageMetadata(), 0, 0)
		if err != nil {
			if p.handleMetadataMismatch(p.env.GetServerContext(), err, fileMetadataKey, fileMetadata) {
				mismatchCount += 1
			}
		}
	}
	log.Infof("Pebble Cache: scanForBrokenFiles fixed %d files", mismatchCount)
	return nil
}

func (p *PebbleCache) MigrateFromDiskDir(diskDir string) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	ch := disk_cache.ScanDiskDirectory(diskDir)

	start := time.Now()
	inserted := 0
	err = p.batchProcessCh(ch, func(batch *pebble.Batch, fileMetadata *rfpb.FileMetadata) error {
		protoBytes, err := proto.Marshal(fileMetadata)
		if err != nil {
			return err
		}
		fileMetadataKey, err := p.fileStorer.FileMetadataKey(fileMetadata.GetFileRecord())
		if err != nil {
			return err
		}
		inserted += 1

		return batch.Set(fileMetadataKey, protoBytes, nil /*ignored write options*/)
	})
	if err != nil {
		return err
	}
	if err := db.Flush(); err != nil {
		return err
	}
	log.Printf("Pebble Cache: Migrated %d files from disk dir %q in %s", inserted, diskDir, time.Since(start))
	return nil
}

type batchEditFn func(batch *pebble.Batch, fileMetadata *rfpb.FileMetadata) error

func (p *PebbleCache) batchProcessCh(ch <-chan *rfpb.FileMetadata, editFn batchEditFn) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	batch := db.NewBatch()
	delta := 0
	for fileMetadata := range ch {
		delta += 1
		if err := editFn(batch, fileMetadata); err != nil {
			log.Warningf("Error editing batch: %s", err)
			continue
		}
		if batch.Count() > 100000 {
			if err := batch.Commit(&pebble.WriteOptions{Sync: true}); err != nil {
				log.Warningf("Error comitting batch: %s", err)
				continue
			}
			batch = db.NewBatch()
		}
	}
	if batch.Count() > 0 {
		if err := batch.Commit(&pebble.WriteOptions{Sync: true}); err != nil {
			log.Warningf("Error comitting batch: %s", err)
		}
	}
	return nil
}

func (p *PebbleCache) Statusz(ctx context.Context) string {
	db, err := p.leaser.DB()
	if err != nil {
		return ""
	}
	defer db.Close()

	p.statusMu.Lock()
	evictors := p.evictors
	p.statusMu.Unlock()

	buf := "<pre>"
	buf += db.Metrics().String()
	diskEstimateBytes, err := db.EstimateDiskUsage(keys.MinByte, keys.MaxByte)
	if err == nil {
		buf += fmt.Sprintf("Estimated pebble DB disk usage: %d bytes\n", diskEstimateBytes)
	}
	var totalSizeBytes, totalCASCount, totalACCount int64
	for _, e := range evictors {
		sizeBytes, casCount, acCount := e.Counts()
		totalSizeBytes += sizeBytes
		totalCASCount += casCount
		totalACCount += acCount
	}
	buf += fmt.Sprintf("[All Partitions] Total Size: %d bytes\n", totalSizeBytes)
	buf += fmt.Sprintf("[All Partitions] CAS total: %d items\n", totalCASCount)
	buf += fmt.Sprintf("[All Partitions] AC total: %d items\n", totalACCount)
	buf += "</pre>"
	for _, e := range evictors {
		buf += e.Statusz(ctx)
	}
	return buf
}

func (p *PebbleCache) lookupGroupAndPartitionID(ctx context.Context, remoteInstanceName string) (string, string, error) {
	auth := p.env.GetAuthenticator()
	if auth == nil {
		return interfaces.AuthAnonymousUser, defaultPartitionID, nil
	}
	user, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser, defaultPartitionID, nil
	}
	for _, pm := range p.partitionMappings {
		if pm.GroupID == user.GetGroupID() && strings.HasPrefix(remoteInstanceName, pm.Prefix) {
			return user.GetGroupID(), pm.PartitionID, nil
		}
	}
	return user.GetGroupID(), defaultPartitionID, nil
}

func (p *PebbleCache) makeFileRecord(ctx context.Context, r *resource.ResourceName) (*rfpb.FileRecord, error) {
	_, err := digest.Validate(r.GetDigest())
	if err != nil {
		return nil, err
	}

	groupID, partID, err := p.lookupGroupAndPartitionID(ctx, r.GetInstanceName())
	if err != nil {
		return nil, err
	}

	return &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:          r.GetCacheType(),
			RemoteInstanceName: r.GetInstanceName(),
			PartitionId:        partID,
			GroupId:            groupID,
		},
		Digest:     r.GetDigest(),
		Compressor: r.GetCompressor(),
	}, nil
}

// blobDir returns a directory path under the root directory where blobs can be stored.
func (p *PebbleCache) blobDir(shouldIncludePartition bool, partID string) string {
	filePath := filepath.Join(p.rootDirectory, "blobs")
	if shouldIncludePartition {
		partDir := partitionDirectoryPrefix + partID
		filePath = filepath.Join(filePath, partDir)
	}
	return filePath
}

func lookupFileMetadata(ctx context.Context, iter *pebble.Iterator, fileMetadataKey []byte) (*rfpb.FileMetadata, error) {
	fileMetadata := &rfpb.FileMetadata{}
	if err := pebbleutil.LookupProto(iter, fileMetadataKey, fileMetadata); err != nil {
		return nil, err
	}

	return fileMetadata, nil
}

func readFileMetadata(reader pebble.Reader, fileMetadataKey []byte) (*rfpb.FileMetadata, error) {
	fileMetadata := &rfpb.FileMetadata{}
	buf, err := pebbleutil.GetCopy(reader, fileMetadataKey)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(buf, fileMetadata); err != nil {
		return nil, err
	}

	return fileMetadata, nil
}

func (p *PebbleCache) handleMetadataMismatch(ctx context.Context, causeErr error, fileMetadataKey []byte, fileMetadata *rfpb.FileMetadata) bool {
	if !status.IsNotFoundError(causeErr) && !os.IsNotExist(causeErr) {
		return false
	}
	if fileMetadata.GetStorageMetadata().GetFileMetadata() != nil {
		err := p.deleteMetadataOnly(ctx, fileMetadataKey)
		if err != nil && status.IsNotFoundError(err) {
			return false
		}
		log.Warningf("Metadata record %q was found but file (%+v) not found on disk: %s", fileMetadataKey, fileMetadata, causeErr)
		if err != nil {
			log.Warningf("Error deleting metadata: %s", err)
			return false
		}
		return true
	}
	return false
}

func (p *PebbleCache) Contains(ctx context.Context, r *resource.ResourceName) (bool, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return false, err
	}
	defer db.Close()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	fileRecord, err := p.makeFileRecord(ctx, r)
	if err != nil {
		return false, err
	}
	fileMetadataKey, err := p.fileStorer.FileMetadataKey(fileRecord)
	if err != nil {
		return false, err
	}
	found := pebbleutil.IterHasKey(iter, fileMetadataKey)
	log.Debugf("Pebble contains %s is %v", string(fileMetadataKey), found)
	return found, nil
}

func (p *PebbleCache) Metadata(ctx context.Context, r *resource.ResourceName) (*interfaces.CacheMetadata, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	fileRecord, err := p.makeFileRecord(ctx, r)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := p.fileStorer.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}
	md, err := lookupFileMetadata(ctx, iter, fileMetadataKey)
	if err != nil {
		return nil, err
	}

	return &interfaces.CacheMetadata{
		StoredSizeBytes:    md.GetStoredSizeBytes(),
		DigestSizeBytes:    md.GetFileRecord().GetDigest().GetSizeBytes(),
		LastModifyTimeUsec: md.GetLastModifyUsec(),
		LastAccessTimeUsec: md.GetLastAccessUsec(),
	}, nil
}

func (p *PebbleCache) FindMissing(ctx context.Context, resources []*resource.ResourceName) ([]*repb.Digest, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	sort.Slice(resources, func(i, j int) bool {
		return resources[i].GetDigest().GetHash() < resources[j].GetDigest().GetHash()
	})

	var missing []*repb.Digest
	for _, r := range resources {
		fileRecord, err := p.makeFileRecord(ctx, r)
		if err != nil {
			return nil, err
		}
		fileMetadataKey, err := p.fileStorer.FileMetadataKey(fileRecord)
		if err != nil {
			return nil, err
		}
		if !pebbleutil.IterHasKey(iter, fileMetadataKey) {
			missing = append(missing, r.GetDigest())
		}
	}
	return missing, nil
}

func (p *PebbleCache) Get(ctx context.Context, r *resource.ResourceName) ([]byte, error) {
	rc, err := p.Reader(ctx, r, 0, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (p *PebbleCache) GetMulti(ctx context.Context, resources []*resource.ResourceName) (map[*repb.Digest][]byte, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	foundMap := make(map[*repb.Digest][]byte, len(resources))
	sort.Slice(resources, func(i, j int) bool {
		return resources[i].GetDigest().GetHash() < resources[j].GetDigest().GetHash()
	})

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	buf := &bytes.Buffer{}
	for _, r := range resources {
		fileRecord, err := p.makeFileRecord(ctx, r)
		if err != nil {
			return nil, err
		}
		fileMetadataKey, err := p.fileStorer.FileMetadataKey(fileRecord)
		if err != nil {
			return nil, err
		}
		fileMetadata := &rfpb.FileMetadata{}
		if err := pebbleutil.LookupProto(iter, fileMetadataKey, fileMetadata); err != nil {
			continue
		}

		rc, err := p.readerForCompressionType(ctx, r, fileMetadataKey, fileMetadata, 0, 0)
		if err != nil {
			if status.IsNotFoundError(err) || os.IsNotExist(err) {
				continue
			}
			return nil, err
		}

		_, copyErr := io.Copy(buf, rc)
		closeErr := rc.Close()
		if copyErr != nil || closeErr != nil {
			continue
		}
		foundMap[r.GetDigest()] = append([]byte{}, buf.Bytes()...)
		buf.Reset()
	}
	return foundMap, nil
}

func (p *PebbleCache) Set(ctx context.Context, r *resource.ResourceName, data []byte) error {
	wc, err := p.Writer(ctx, r)
	if err != nil {
		return err
	}
	defer wc.Close()
	if _, err := wc.Write(data); err != nil {
		return err
	}
	return wc.Commit()
}

func (p *PebbleCache) SetMulti(ctx context.Context, kvs map[*resource.ResourceName][]byte) error {
	for r, data := range kvs {
		if err := p.Set(ctx, r, data); err != nil {
			return err
		}
	}
	return nil
}

func (p *PebbleCache) sendSizeUpdate(partID string, fileMetadataKey []byte, delta int64) {
	fileMetadataKeyCopy := make([]byte, len(fileMetadataKey))
	copy(fileMetadataKeyCopy, fileMetadataKey)
	up := &sizeUpdate{
		partID: partID,
		key:    fileMetadataKeyCopy,
		delta:  delta,
	}
	p.edits <- up
}

func sendAtimeUpdate(accesses chan<- *accessTimeUpdate, fileMetadataKey []byte, fileMetadata *rfpb.FileMetadata, updateThreshold time.Duration, atimeBufferSize int) {
	atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
	if !olderThanThreshold(atime, updateThreshold) {
		return
	}

	fileMetadataKeyCopy := make([]byte, len(fileMetadataKey))
	copy(fileMetadataKeyCopy, fileMetadataKey)
	up := &accessTimeUpdate{fileMetadataKeyCopy}

	// If the atimeBufferSize is 0, non-blocking writes do not make sense,
	// so in that case just do a regular channel send. Otherwise; use a non-
	// blocking channel send.
	if atimeBufferSize == 0 {
		accesses <- up
	} else {
		select {
		case accesses <- up:
			return
		default:
			log.Warningf("Dropping atime update for %q", fileMetadataKey)
		}
	}
}

func (p *PebbleCache) deleteMetadataOnly(ctx context.Context, fileMetadataKey []byte) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	// First, lookup the FileMetadata. If it's not found, we don't have the file.
	fileMetadata, err := lookupFileMetadata(ctx, iter, fileMetadataKey)
	if err != nil {
		return err
	}

	if err := db.Delete(fileMetadataKey, &pebble.WriteOptions{Sync: false}); err != nil {
		return err
	}
	p.sendSizeUpdate(fileMetadata.GetFileRecord().GetIsolation().GetPartitionId(), fileMetadataKey, -1*fileMetadata.GetStoredSizeBytes())
	return nil
}

func (p *PebbleCache) deleteRecord(ctx context.Context, fileRecord *rfpb.FileRecord) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	// First, lookup the FileMetadata. If it's not found, we don't have the file.
	fileMetadataKey, err := p.fileStorer.FileMetadataKey(fileRecord)
	if err != nil {
		return err
	}
	fileMetadata, err := lookupFileMetadata(ctx, iter, fileMetadataKey)
	if err != nil {
		return err
	}

	partitionID := fileRecord.GetIsolation().GetPartitionId()
	blobDir := p.blobDir(!p.isolateByGroupIDs, partitionID)
	fp := p.fileStorer.FilePath(blobDir, fileMetadata.GetStorageMetadata().GetFileMetadata())
	if err := db.Delete(fileMetadataKey, &pebble.WriteOptions{Sync: false}); err != nil {
		return err
	}
	p.sendSizeUpdate(partitionID, fileMetadataKey, -1*fileMetadata.GetStoredSizeBytes())
	if err := disk.DeleteFile(ctx, fp); err != nil {
		return err
	}
	parentDir := filepath.Dir(fp)
	if err := deleteDirIfEmptyAndOld(parentDir); err != nil {
		log.Debugf("Error deleting dir: %s: %s", parentDir, err)
	}
	return nil
}

func (p *PebbleCache) Delete(ctx context.Context, r *resource.ResourceName) error {
	fileRecord, err := p.makeFileRecord(ctx, r)
	if err != nil {
		return err
	}
	return p.deleteRecord(ctx, fileRecord)
}

func (p *PebbleCache) Reader(ctx context.Context, r *resource.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	fileRecord, err := p.makeFileRecord(ctx, r)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := p.fileStorer.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}
	log.Debugf("Attempting pebble reader %s", string(fileMetadataKey))

	// First, lookup the FileMetadata. If it's not found, we don't have the file.
	fileMetadata, err := lookupFileMetadata(ctx, iter, fileMetadataKey)
	if err != nil {
		return nil, err
	}

	rc, err := p.readerForCompressionType(ctx, r, fileMetadataKey, fileMetadata, uncompressedOffset, limit)
	if err != nil {
		return nil, err
	}

	// Grab another lease and pass the Close function to the reader
	// so it will be closed when the reader is.
	db, err = p.leaser.DB()
	if err != nil {
		return nil, err
	}
	return pebbleutil.ReadCloserWithFunc(rc, db.Close), nil
}

type writeCloser struct {
	interfaces.MetadataWriteCloser
	closeFn      func(n int64) error
	bytesWritten int64
}

func (dc *writeCloser) Close() error {
	if err := dc.MetadataWriteCloser.Close(); err != nil {
		return err
	}
	return dc.closeFn(dc.bytesWritten)
}

func (dc *writeCloser) Write(p []byte) (int, error) {
	n, err := dc.MetadataWriteCloser.Write(p)
	if err != nil {
		return 0, err
	}
	dc.bytesWritten += int64(n)
	return n, nil
}

// zstdCompressor compresses bytes before writing them to the nested writer
type zstdCompressor struct {
	cacheName string

	*ioutil.CustomCommitWriteCloser
	compressBuf []byte
	bufferPool  *bytebufferpool.Pool

	numDecompressedBytes int
	numCompressedBytes   int
}

func NewZstdCompressor(cacheName string, wc *ioutil.CustomCommitWriteCloser, bp *bytebufferpool.Pool, digestSize int64) *zstdCompressor {
	compressBuf := bp.Get(digestSize)
	return &zstdCompressor{
		cacheName:               cacheName,
		CustomCommitWriteCloser: wc,
		compressBuf:             compressBuf,
		bufferPool:              bp,
	}
}

func (z *zstdCompressor) Write(decompressedBytes []byte) (int, error) {
	z.compressBuf = compression.CompressZstd(z.compressBuf, decompressedBytes)
	compressedBytesWritten, err := z.CustomCommitWriteCloser.Write(z.compressBuf)
	if err != nil {
		return 0, err
	}

	z.numDecompressedBytes += len(decompressedBytes)
	z.numCompressedBytes += compressedBytesWritten

	// Return the size of the original buffer even though a different compressed buffer size may have been written,
	// or clients will return a short write error
	return len(decompressedBytes), nil
}

func (z *zstdCompressor) Close() error {
	metrics.CompressionRatio.
		With(prometheus.Labels{metrics.CompressionType: "zstd", metrics.CacheNameLabel: z.cacheName}).
		Observe(float64(z.numCompressedBytes) / float64(z.numDecompressedBytes))

	z.bufferPool.Put(z.compressBuf)
	return z.CustomCommitWriteCloser.Close()
}

func (p *PebbleCache) Writer(ctx context.Context, r *resource.ResourceName) (interfaces.CommittedWriteCloser, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// If data is not already compressed, return a writer that will compress it before writing
	// Only compress data over a given size for more optimal compression ratios
	shouldCompress := p.enableZstdCompression &&
		r.GetCompressor() == repb.Compressor_IDENTITY &&
		r.GetDigest().GetSizeBytes() >= p.minBytesAutoZstdCompression
	if shouldCompress {
		r = &resource.ResourceName{
			Digest:       r.GetDigest(),
			InstanceName: r.GetInstanceName(),
			Compressor:   repb.Compressor_ZSTD,
			CacheType:    r.GetCacheType(),
		}
	}

	fileRecord, err := p.makeFileRecord(ctx, r)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := p.fileStorer.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}
	log.Debugf("Attempting pebble writer %s", string(fileMetadataKey))

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()
	alreadyExists := pebbleutil.IterHasKey(iter, fileMetadataKey)
	if alreadyExists {
		metrics.DiskCacheDuplicateWrites.With(prometheus.Labels{metrics.CacheNameLabel: p.name}).Inc()
		metrics.DiskCacheDuplicateWritesBytes.With(prometheus.Labels{metrics.CacheNameLabel: p.name}).Add(float64(r.GetDigest().GetSizeBytes()))
	}

	var wcm interfaces.MetadataWriteCloser
	if r.GetDigest().GetSizeBytes() < p.maxInlineFileSizeBytes {
		wcm = p.fileStorer.InlineWriter(ctx, r.GetDigest().GetSizeBytes())
	} else {
		partitionID := fileRecord.GetIsolation().GetPartitionId()
		blobDir := p.blobDir(!p.isolateByGroupIDs, partitionID)
		fw, err := p.fileStorer.FileWriter(ctx, blobDir, fileRecord)
		if err != nil {
			return nil, err
		}
		wcm = fw
	}

	// Grab another lease and pass the Close function to the writer
	// so it will be closed when the writer is.
	db, err = p.leaser.DB()
	if err != nil {
		return nil, err
	}
	wc := ioutil.NewCustomCommitWriteCloser(wcm)
	wc.CloseFn = db.Close
	wc.CommitFn = func(bytesWritten int64) error {
		now := time.Now().UnixMicro()
		md := &rfpb.FileMetadata{
			FileRecord:      fileRecord,
			StorageMetadata: wcm.Metadata(),
			StoredSizeBytes: bytesWritten,
			LastAccessUsec:  now,
			LastModifyUsec:  now,
		}
		protoBytes, err := proto.Marshal(md)
		if err != nil {
			return err
		}

		err = db.Set(fileMetadataKey, protoBytes, &pebble.WriteOptions{Sync: false})
		if err == nil {
			partitionID := fileRecord.GetIsolation().GetPartitionId()
			p.sendSizeUpdate(partitionID, fileMetadataKey, bytesWritten)
			metrics.DiskCacheAddedFileSizeBytes.With(prometheus.Labels{metrics.CacheNameLabel: p.name}).Observe(float64(bytesWritten))
		}

		return err
	}

	if shouldCompress {
		return NewZstdCompressor(p.name, wc, p.bufferPool, r.GetDigest().GetSizeBytes()), nil
	}

	return wc, nil
}

func (p *PebbleCache) DoneScanning() bool {
	var brokenFilesDone, orphanedFilesDone bool

	select {
	case <-p.brokenFilesDone:
		brokenFilesDone = true
	default:
		break
	}

	select {
	case <-p.orphanedFilesDone:
		orphanedFilesDone = true
	default:
		break
	}

	return brokenFilesDone && orphanedFilesDone
}

// TestingWaitForGC should be used by tests only.
// This function waits until any active file deletion has finished.
func (p *PebbleCache) TestingWaitForGC() error {
	for {
		p.statusMu.Lock()
		evictors := p.evictors
		p.statusMu.Unlock()

		done := 0
		for _, e := range evictors {
			e.mu.Lock()
			maxAllowedSize := int64(JanitorCutoffThreshold * float64(e.part.MaxSizeBytes))
			totalSizeBytes := e.sizeBytes
			e.mu.Unlock()

			if totalSizeBytes <= maxAllowedSize {
				done += 1
			}
		}
		if done == len(evictors) {
			break
		}
	}
	return nil
}

type evictionPoolEntry struct {
	timestamp       int64
	fileMetadata    *rfpb.FileMetadata
	fileMetadataKey []byte
}

type partitionEvictor struct {
	mu                *sync.Mutex
	part              disk.Partition
	fileStorer        filestore.Store
	cacheName         string
	blobDir           string
	dbGetter          pebbleutil.Leaser
	accesses          chan<- *accessTimeUpdate
	rng               *rand.Rand
	isolateByGroupIDs bool

	samplePool  []*evictionPoolEntry
	sizeBytes   int64
	casCount    int64
	acCount     int64
	lastRun     time.Time
	lastEvicted *evictionPoolEntry

	atimeBufferSize int
	minEvictionAge  time.Duration
}

func newPartitionEvictor(part disk.Partition, fileStorer filestore.Store, blobDir string, dbg pebbleutil.Leaser, accesses chan<- *accessTimeUpdate, atimeBufferSize int, minEvictionAge time.Duration, cacheName string, isolateByGroupIDs bool) (*partitionEvictor, error) {
	pe := &partitionEvictor{
		mu:                &sync.Mutex{},
		part:              part,
		fileStorer:        fileStorer,
		blobDir:           blobDir,
		samplePool:        make([]*evictionPoolEntry, 0, *samplePoolSize),
		dbGetter:          dbg,
		accesses:          accesses,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		atimeBufferSize:   atimeBufferSize,
		minEvictionAge:    minEvictionAge,
		cacheName:         cacheName,
		isolateByGroupIDs: isolateByGroupIDs,
	}
	start := time.Now()
	log.Infof("Pebble Cache: Initializing cache partition %q...", part.ID)
	sizeBytes, casCount, acCount, err := pe.computeSize()
	if err != nil {
		return nil, err
	}
	pe.sizeBytes = sizeBytes
	pe.casCount = casCount
	pe.acCount = acCount

	log.Infof("Pebble Cache: Initialized cache partition %q AC: %d, CAS: %d, Size: %d [bytes] in %s", part.ID, pe.acCount, pe.casCount, pe.sizeBytes, time.Since(start))
	return pe, nil
}

func (e *partitionEvictor) updateSize(fileMetadataKey []byte, deltaSize int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	deltaCount := int64(1)
	if deltaSize < 0 {
		deltaCount = -1
	}

	if bytes.Contains(fileMetadataKey, casDir) {
		e.casCount += deltaCount
	} else if bytes.Contains(fileMetadataKey, acDir) {
		e.acCount += deltaCount
	} else {
		log.Warningf("Unidentified file (not CAS or AC): %q", fileMetadataKey)
	}
	e.sizeBytes += deltaSize
	lbls := prometheus.Labels{metrics.PartitionID: e.part.ID, metrics.CacheNameLabel: e.cacheName}
	metrics.DiskCachePartitionSizeBytes.With(lbls).Set(float64(e.sizeBytes))
	metrics.DiskCachePartitionCapacityBytes.With(lbls).Set(float64(e.part.MaxSizeBytes))

	metrics.DiskCachePartitionNumItems.With(prometheus.Labels{
		metrics.PartitionID:    e.part.ID,
		metrics.CacheNameLabel: e.cacheName,
		metrics.CacheTypeLabel: "ac"}).Set(float64(e.acCount))
	metrics.DiskCachePartitionNumItems.With(prometheus.Labels{
		metrics.PartitionID:    e.part.ID,
		metrics.CacheNameLabel: e.cacheName,
		metrics.CacheTypeLabel: "cas"}).Set(float64(e.casCount))
}

func (e *partitionEvictor) computeSizeInRange(start, end []byte) (int64, int64, int64, error) {
	db, err := e.dbGetter.DB()
	if err != nil {
		return 0, 0, 0, err
	}
	defer db.Close()
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	defer iter.Close()
	iter.SeekLT(start)

	casCount := int64(0)
	acCount := int64(0)
	blobSizeBytes := int64(0)
	metadataSizeBytes := int64(0)
	fileMetadata := &rfpb.FileMetadata{}

	for iter.Next() {
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			return 0, 0, 0, err
		}
		blobSizeBytes += fileMetadata.GetStoredSizeBytes()
		metadataSizeBytes += int64(len(iter.Value()))

		// identify and count CAS vs AC files.
		if bytes.Contains(iter.Key(), casDir) {
			casCount += 1
		} else if bytes.Contains(iter.Key(), acDir) {
			acCount += 1
		} else {
			log.Warningf("Unidentified file (not CAS or AC): %q", iter.Key())
		}
	}

	return blobSizeBytes + metadataSizeBytes, casCount, acCount, nil
}

func splitRange(left, right []byte, count int) ([][]byte, error) {
	leftInt := big.NewInt(0).SetBytes(left)
	rightInt := big.NewInt(0).SetBytes(right)
	delta := new(big.Int).Sub(rightInt, leftInt)
	interval := new(big.Int).Div(delta, big.NewInt(int64(count)))
	if interval.Sign() != 1 {
		return nil, status.InvalidArgumentErrorf("delta (%s) < count (%d)", delta, count)
	}

	ranges := make([][]byte, 0, count)

	l := leftInt
	ranges = append(ranges, leftInt.Bytes())
	for i := 0; i < count; i++ {
		r := new(big.Int).Add(l, interval)
		ranges = append(ranges, r.Bytes())
		l = r
	}
	ranges = append(ranges, rightInt.Bytes())
	return ranges, nil
}

func (e *partitionEvictor) partitionMetadataKey() []byte {
	var key []byte
	key = append(key, SystemKeyPrefix...)
	key = append(key, []byte(e.part.ID)...)
	key = append(key, []byte("/metadata")...)
	return key
}

func (e *partitionEvictor) lookupPartitionMetadata() (*rfpb.PartitionMetadata, error) {
	db, err := e.dbGetter.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	partitionMDBuf, err := pebbleutil.GetCopy(db, e.partitionMetadataKey())
	if err != nil {
		return nil, err
	}

	partitionMD := &rfpb.PartitionMetadata{}
	if err := proto.Unmarshal(partitionMDBuf, partitionMD); err != nil {
		return nil, err
	}
	return partitionMD, nil
}

func (e *partitionEvictor) writePartitionMetadata(db pebbleutil.IPebbleDB, md *rfpb.PartitionMetadata) error {
	bs, err := proto.Marshal(md)
	if err != nil {
		return err
	}
	return db.Set(e.partitionMetadataKey(), bs, &pebble.WriteOptions{Sync: true})
}

func (e *partitionEvictor) flushPartitionMetadata(db pebbleutil.IPebbleDB) error {
	sizeBytes, casCount, acCount := e.Counts()
	return e.writePartitionMetadata(db, &rfpb.PartitionMetadata{
		SizeBytes: sizeBytes,
		CasCount:  casCount,
		AcCount:   acCount,
	})
}

func (e *partitionEvictor) computeSize() (int64, int64, int64, error) {
	if !*forceCalculateMetadata {
		partitionMD, err := e.lookupPartitionMetadata()
		if err == nil {
			log.Infof("Loaded partition %q metadata from cache: %+v", e.part.ID, partitionMD)
			return partitionMD.GetSizeBytes(), partitionMD.GetCasCount(), partitionMD.GetAcCount(), nil
		} else if !status.IsNotFoundError(err) {
			return 0, 0, 0, err
		}
	}

	start := append([]byte(e.partitionKeyPrefix()+"/"), keys.MinByte...)
	end := append([]byte(e.partitionKeyPrefix()+"/"), keys.MaxByte...)
	totalSizeBytes, totalCasCount, totalAcCount, err := e.computeSizeInRange(start, end)

	partitionMD := &rfpb.PartitionMetadata{
		PartitionId: e.part.ID,
		SizeBytes:   totalSizeBytes,
		CasCount:    totalCasCount,
		AcCount:     totalAcCount,
		TotalCount:  totalCasCount + totalAcCount,
	}

	db, err := e.dbGetter.DB()
	if err != nil {
		return 0, 0, 0, err
	}
	defer db.Close()
	if err := e.writePartitionMetadata(db, partitionMD); err != nil {
		return 0, 0, 0, err
	}

	return totalSizeBytes, totalCasCount, totalAcCount, nil
}

func (e *partitionEvictor) Counts() (int64, int64, int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sizeBytes, e.casCount, e.acCount
}

func (e *partitionEvictor) Statusz(ctx context.Context) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	buf := "<pre>"
	buf += fmt.Sprintf("Partition %q (%q)\n", e.part.ID, e.blobDir)

	maxAllowedSize := int64(JanitorCutoffThreshold * float64(e.part.MaxSizeBytes))
	percentFull := float64(e.sizeBytes) / float64(maxAllowedSize) * 100.0
	totalCount := e.casCount + e.acCount
	buf += fmt.Sprintf("Items: CAS: %d AC: %d (%d total)\n", e.casCount, e.acCount, totalCount)
	buf += fmt.Sprintf("Usage: %s / %s (%2.2f%% full)\n", units.BytesSize(float64(e.sizeBytes)), units.BytesSize(float64(maxAllowedSize)), percentFull)
	buf += fmt.Sprintf("GC Last run: %s\n", e.lastRun.Format("Jan 02, 2006 15:04:05 MST"))
	lastEvictedStr := "nil"
	if e.lastEvicted != nil {
		age := time.Since(time.Unix(0, e.lastEvicted.timestamp))
		lastEvictedStr = fmt.Sprintf("%q age: %s", e.lastEvicted.fileMetadataKey, age)
	}
	buf += fmt.Sprintf("Last evicted item: %s\n", lastEvictedStr)
	buf += "</pre>"
	return buf
}

var digestChars = []byte("abcdef1234567890")

func (e *partitionEvictor) randomKey(n int) []byte {
	partID := e.partitionKeyPrefix()
	cacheType := "/cas/"
	buf := bytes.NewBuffer(make([]byte, 0, len(partID)+len(cacheType)+n))
	buf.Write([]byte(partID))
	buf.Write([]byte(cacheType))
	for i := 0; i < n; i++ {
		buf.WriteByte(digestChars[e.rng.Intn(len(digestChars))])
	}
	return buf.Bytes()
}

func (e *partitionEvictor) refreshAtime(s *evictionPoolEntry) error {
	if s.fileMetadata.GetLastAccessUsec() == 0 {
		sendAtimeUpdate(e.accesses, s.fileMetadataKey, s.fileMetadata, 0 /*force an update*/, e.atimeBufferSize)
		return status.FailedPreconditionErrorf("File %q had no atime set", s.fileMetadataKey)
	}
	atime := time.UnixMicro(s.fileMetadata.GetLastAccessUsec())
	age := time.Since(atime)
	if age < e.minEvictionAge {
		return status.FailedPreconditionErrorf("File %q was not old enough: age %s", s.fileMetadataKey, age)
	}
	s.timestamp = atime.UnixNano()
	return nil
}

func (e *partitionEvictor) randomSample(iter *pebble.Iterator, k int) ([]*evictionPoolEntry, error) {
	samples := make([]*evictionPoolEntry, 0, k)
	seen := make(map[string]struct{}, len(e.samplePool))
	for _, entry := range e.samplePool {
		seen[string(entry.fileMetadataKey)] = struct{}{}
	}

	// generate k random digests and for each:
	//   - seek to the next valid key, and return that file record
	for i := 0; i < k*2; i++ {
		randKey := e.randomKey(64)
		valid := iter.SeekGE(randKey)
		if !valid {
			continue
		}
		fileMetadata := &rfpb.FileMetadata{}
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			return nil, err
		}
		if _, ok := seen[string(iter.Key())]; ok {
			continue
		}
		seen[string(iter.Key())] = struct{}{}

		fileMetadataKey := make([]byte, len(iter.Key()))
		copy(fileMetadataKey, iter.Key())

		sample := &evictionPoolEntry{
			fileMetadata:    fileMetadata,
			fileMetadataKey: fileMetadataKey,
		}
		if err := e.refreshAtime(sample); err != nil {
			continue
		}

		samples = append(samples, sample)
		if len(samples) == k {
			break
		}
	}

	return samples, nil
}

func deleteDirIfEmptyAndOld(dir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	di, err := os.Stat(dir)
	if err != nil {
		return err
	}

	if len(files) != 0 || time.Since(di.ModTime()) < *dirDeletionDelay {
		// dir was not empty or was too young
		return nil
	}

	return os.Remove(dir)
}

func (e *partitionEvictor) deleteFile(sample *evictionPoolEntry) error {
	db, err := e.dbGetter.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Delete(sample.fileMetadataKey, &pebble.WriteOptions{Sync: true}); err != nil {
		return err
	}

	md := sample.fileMetadata.GetStorageMetadata()
	switch {
	case md.GetFileMetadata() != nil:
		fp := e.fileStorer.FilePath(e.blobDir, md.GetFileMetadata())
		if err := disk.DeleteFile(context.TODO(), fp); err != nil {
			return err
		}
		parentDir := filepath.Dir(fp)
		if err := deleteDirIfEmptyAndOld(parentDir); err != nil {
			log.Debugf("Error deleting dir: %s: %s", parentDir, err)
		}
	case md.GetInlineMetadata() != nil:
		break
	default:
		return status.FailedPreconditionErrorf("Unnown storage metadata type: %+v", md)
	}

	ageUsec := float64(time.Since(time.Unix(0, sample.timestamp)).Microseconds())
	metrics.DiskCacheLastEvictionAgeUsec.With(prometheus.Labels{metrics.PartitionID: e.part.ID, metrics.CacheNameLabel: e.cacheName}).Set(ageUsec)
	e.updateSize(sample.fileMetadataKey, -1*sample.fileMetadata.GetStoredSizeBytes())
	return nil
}

func (e *partitionEvictor) partitionKeyPrefix() string {
	if e.isolateByGroupIDs {
		return filestore.PartitionDirectoryPrefix + e.part.ID
	}
	return e.part.ID
}

func (e *partitionEvictor) resampleK(k int) error {
	db, err := e.dbGetter.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	start, end := keyRange([]byte(e.partitionKeyPrefix() + "/"))
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	iter.SeekGE(start)
	defer iter.Close()

	// read new entries to put in the pool.
	additions := make([]*evictionPoolEntry, 0, k)
	for i := 0; i < k; i++ {
		entries, err := e.randomSample(iter, *samplesPerEviction)
		if err != nil {
			return err
		}
		additions = append(additions, entries...)
	}

	filtered := make([]*evictionPoolEntry, 0, len(e.samplePool))

	// refresh all the entries already in the pool.
	for _, sample := range e.samplePool {
		fm, err := readFileMetadata(db, sample.fileMetadataKey)
		if err != nil {
			continue
		}
		sample.fileMetadata = fm
		if err := e.refreshAtime(sample); err != nil {
			continue
		}
		filtered = append(filtered, sample)
	}

	e.samplePool = append(filtered, additions...)

	if len(e.samplePool) > 0 {
		sort.Slice(e.samplePool, func(i, j int) bool {
			return e.samplePool[i].timestamp < e.samplePool[j].timestamp
		})
	}

	if len(e.samplePool) > *samplePoolSize {
		e.samplePool = e.samplePool[:*samplePoolSize]
	}

	return nil
}

// evict is based off the Redis approximated LRU algorithm
func (e *partitionEvictor) evict(count int) (*evictionPoolEntry, error) {
	db, err := e.dbGetter.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	evicted := 0
	var lastEvicted *evictionPoolEntry
	for n := 0; n < count; n++ {
		lastCount := evicted

		// Resample every time we evict a key
		if err := e.resampleK(1); err != nil {
			return nil, err
		}
		for i, sample := range e.samplePool {
			_, closer, err := db.Get(sample.fileMetadataKey)
			if err == pebble.ErrNotFound {
				continue
			}
			closer.Close()
			age := time.Since(time.Unix(0, sample.timestamp))
			log.Infof("Evictor %q attempting to evict key %q (last accessed %s)", e.part.ID, sample.fileMetadataKey, age)
			if err := e.deleteFile(sample); err != nil {
				log.Errorf("Error evicting file for key %q: %s (ignoring)", sample.fileMetadataKey, err)
				continue
			}
			lbls := prometheus.Labels{metrics.PartitionID: e.part.ID, metrics.CacheNameLabel: e.cacheName}
			metrics.DiskCacheNumEvictions.With(lbls).Inc()
			metrics.DiskCacheEvictionAgeMsec.With(lbls).Observe(float64(age.Milliseconds()))
			evicted += 1
			lastEvicted = sample
			e.samplePool = append(e.samplePool[:i], e.samplePool[i+1:]...)
			break
		}

		// If no candidates were evictable in the whole pool, resample
		// the pool.
		if lastCount == evicted {
			e.samplePool = e.samplePool[:0]
			if err := e.resampleK(*samplePoolSize); err != nil {
				return nil, err
			}
		}

		if count == evicted {
			break
		}
	}
	return lastEvicted, nil
}

func (e *partitionEvictor) ttl(quitChan chan struct{}) {
	e.mu.Lock()
	maxAllowedSize := int64(JanitorCutoffThreshold * float64(e.part.MaxSizeBytes))
	e.mu.Unlock()

	for {
		e.mu.Lock()
		sizeBytes := e.sizeBytes
		totalCount := e.casCount + e.acCount
		e.mu.Unlock()

		if sizeBytes <= maxAllowedSize {
			break
		}

		select {
		case <-quitChan:
			return
		default:
			break
		}

		numToEvict := int(.001 * float64(totalCount))
		if numToEvict == 0 {
			numToEvict = 1
		}

		lastEvicted, err := e.evict(numToEvict)
		if err != nil {
			log.Warningf("Evictor %q couldn't run: %s", e.part.ID, err)
			time.Sleep(time.Second)
			continue
		}

		// If we attempted to evict and were unable to, sleep for a
		// bit before trying again.
		if lastEvicted == nil {
			time.Sleep(time.Second)
		}

		e.mu.Lock()
		e.lastRun = time.Now()
		e.lastEvicted = lastEvicted
		e.mu.Unlock()
	}
}

func (e *partitionEvictor) run(quitChan chan struct{}) error {
	for {
		select {
		case <-quitChan:
			return nil
		case <-time.After(JanitorCheckPeriod):
			e.ttl(quitChan)
		}
	}
}

func (p *PebbleCache) flushPartitionMetadata() {
	for _, e := range p.evictors {
		if err := e.flushPartitionMetadata(p.db); err != nil {
			log.Warningf("could not flush partition metadata: %s", err)
		}
	}
}

func (p *PebbleCache) periodicFlushPartitionMetadata(quitChan chan struct{}) {
	for {
		select {
		case <-quitChan:
			return
		case <-time.After(partitionMetadataFlushPeriod):
			p.flushPartitionMetadata()
		}
	}
}

func (p *PebbleCache) refreshMetrics(quitChan chan struct{}) {
	for {
		select {
		case <-quitChan:
			return
		case <-time.After(metricsRefreshPeriod):
			fsu := gosigar.FileSystemUsage{}
			if err := fsu.Get(p.rootDirectory); err != nil {
				log.Warningf("could not retrieve filesystem stats: %s", err)
			} else {
				metrics.DiskCacheFilesystemTotalBytes.With(prometheus.Labels{metrics.CacheNameLabel: p.name}).Set(float64(fsu.Total))
				metrics.DiskCacheFilesystemAvailBytes.With(prometheus.Labels{metrics.CacheNameLabel: p.name}).Set(float64(fsu.Avail))
			}
		}
	}
}

func (p *PebbleCache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	switch compressor {
	case repb.Compressor_IDENTITY:
		return true
	case repb.Compressor_ZSTD:
		return p.enableZstdCompression
	default:
		return false
	}
}

// compressionReader helps manage resources associated with a compression.NewZstdCompressingReader
type compressionReader struct {
	io.ReadCloser
	readBuf     []byte
	compressBuf []byte
	bufferPool  *bytebufferpool.Pool
}

func (r *compressionReader) Close() error {
	err := r.ReadCloser.Close()
	r.bufferPool.Put(r.readBuf)
	r.bufferPool.Put(r.compressBuf)
	return err
}

type readCloser struct {
	io.Reader
	io.Closer
}

func (p *PebbleCache) readerForCompressionType(ctx context.Context, resource *resource.ResourceName, fileMetadataKey []byte, fileMetadata *rfpb.FileMetadata, uncompressedOffset int64, uncompressedLimit int64) (io.ReadCloser, error) {
	partitionID := fileMetadata.GetFileRecord().GetIsolation().GetPartitionId()
	blobDir := p.blobDir(!p.isolateByGroupIDs, partitionID)
	requestedCompression := resource.GetCompressor()
	cachedCompression := fileMetadata.GetFileRecord().GetCompressor()

	// If the data is stored uncompressed, we can use the offset/limit directly
	// otherwise we need to decompress first.
	offset := int64(0)
	limit := int64(0)
	if cachedCompression == repb.Compressor_IDENTITY {
		offset = uncompressedOffset
		limit = uncompressedLimit
	}
	reader, err := p.fileStorer.NewReader(ctx, blobDir, fileMetadata.GetStorageMetadata(), offset, limit)
	if err != nil {
		if status.IsNotFoundError(err) || os.IsNotExist(err) {
			p.handleMetadataMismatch(ctx, err, fileMetadataKey, fileMetadata)
		}
		return nil, err
	}
	sendAtimeUpdate(p.accesses, fileMetadataKey, fileMetadata, p.atimeUpdateThreshold, p.atimeBufferSize)

	if requestedCompression == cachedCompression {
		if requestedCompression != repb.Compressor_IDENTITY && (uncompressedOffset != 0 || uncompressedLimit != 0) {
			return nil, status.FailedPreconditionError("passthrough compression does not support offset/limit")
		}
		return reader, nil
	}

	if requestedCompression == repb.Compressor_ZSTD && cachedCompression == repb.Compressor_IDENTITY {
		bufSize := int64(CompressorBufSizeBytes)
		resourceSize := resource.GetDigest().GetSizeBytes()
		if resourceSize > 0 && resourceSize < bufSize {
			bufSize = resourceSize
		}

		readBuf := p.bufferPool.Get(bufSize)
		compressBuf := p.bufferPool.Get(bufSize)

		cr, err := compression.NewZstdCompressingReader(reader, readBuf[:bufSize], compressBuf[:bufSize])
		if err != nil {
			p.bufferPool.Put(readBuf)
			p.bufferPool.Put(compressBuf)
			return nil, err
		}
		return &compressionReader{
			ReadCloser:  cr,
			readBuf:     readBuf,
			compressBuf: compressBuf,
			bufferPool:  p.bufferPool,
		}, err
	} else if requestedCompression == repb.Compressor_IDENTITY && cachedCompression == repb.Compressor_ZSTD {
		dr, err := compression.NewZstdDecompressingReader(reader)
		if err != nil {
			return nil, err
		}
		// If offset is set, we need to discard all the bytes before that point.
		if uncompressedOffset != 0 {
			if _, err := io.CopyN(io.Discard, dr, uncompressedOffset); err != nil {
				_ = dr.Close()
				return nil, err
			}
		}
		if uncompressedLimit != 0 {
			dr = &readCloser{io.LimitReader(dr, uncompressedLimit), dr}
		}
		return dr, nil
	} else {
		return nil, fmt.Errorf("unsupported compressor %v requested for %v reader, cached compression is %v",
			requestedCompression, resource, cachedCompression)
	}
}

func (p *PebbleCache) Start() error {
	p.quitChan = make(chan struct{}, 0)
	for _, evictor := range p.evictors {
		evictor := evictor
		p.eg.Go(func() error {
			return evictor.run(p.quitChan)
		})
	}
	p.eg.Go(func() error {
		p.periodicFlushPartitionMetadata(p.quitChan)
		return nil
	})
	p.egSizeUpdates.Go(func() error {
		p.processSizeUpdates()
		return nil
	})
	p.eg.Go(func() error {
		return p.processAccessTimeUpdates(p.quitChan)
	})
	p.eg.Go(func() error {
		return p.scanForBrokenFiles(p.quitChan)
	})
	if *scanForOrphanedFiles {
		p.eg.Go(func() error {
			return p.deleteOrphanedFiles(p.quitChan)
		})
	}
	p.eg.Go(func() error {
		p.refreshMetrics(p.quitChan)
		return nil
	})
	return nil
}

func (p *PebbleCache) Stop() error {
	log.Info("Pebble Cache: beginning shutdown")
	close(p.quitChan)
	if err := p.eg.Wait(); err != nil {
		return err
	}
	log.Info("Pebble Cache: waitgroups finished")

	// Wait for all active requests to be finished.
	p.leaser.Close()

	log.Infof("Pebble Cache: finished serving requests")

	// Wait for all enqueued size updates to be processed.
	close(p.edits)
	if err := p.egSizeUpdates.Wait(); err != nil {
		return err
	}

	log.Infof("Pebble Cache: finished processing size updates")

	// Write out the final partition metadata.
	p.flushPartitionMetadata()

	if err := p.db.Flush(); err != nil {
		return err
	}
	log.Infof("Pebble Cache: db flushed")

	return p.db.Close()
}
