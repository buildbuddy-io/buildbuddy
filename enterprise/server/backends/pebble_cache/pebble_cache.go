package pebble_cache

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/chunker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockmap"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/pbwireutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/docker/go-units"
	"github.com/elastic/gosigar"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

var (
	nameFlag                   = flag.String("cache.pebble.name", DefaultName, "The name used in reporting cache metrics and status.")
	rootDirectoryFlag          = flag.String("cache.pebble.root_directory", "", "The root directory to store the database in.")
	blockCacheSizeBytesFlag    = flag.Int64("cache.pebble.block_cache_size_bytes", DefaultBlockCacheSizeBytes, "How much ram to give the block cache")
	maxInlineFileSizeBytesFlag = flag.Int64("cache.pebble.max_inline_file_size_bytes", DefaultMaxInlineFileSizeBytes, "Files smaller than this may be inlined directly into pebble")
	partitionsFlag             = flag.Slice("cache.pebble.partitions", []disk.Partition{}, "")
	partitionMappingsFlag      = flag.Slice("cache.pebble.partition_mappings", []disk.PartitionMapping{}, "")

	backgroundRepairFrequency = flag.Duration("cache.pebble.background_repair_frequency", 1*24*time.Hour, "How frequently to run period background repair tasks.")
	backgroundRepairQPSLimit  = flag.Int("cache.pebble.background_repair_qps_limit", 100, "QPS limit for background repair modifications.")
	deleteACEntriesOlderThan  = flag.Duration("cache.pebble.delete_ac_entries_older_than", 0, "If set, the background repair will delete AC entries older than this time.")
	scanForMissingFiles       = flag.Bool("cache.pebble.scan_for_missing_files", false, "If set, scan all keys and check if external files are missing on disk. Deletes keys with missing files.")
	scanForOrphanedFiles      = flag.Bool("cache.pebble.scan_for_orphaned_files", false, "If true, scan for orphaned files")
	orphanDeleteDryRun        = flag.Bool("cache.pebble.orphan_delete_dry_run", true, "If set, log orphaned files instead of deleting them")
	dirDeletionDelay          = flag.Duration("cache.pebble.dir_deletion_delay", time.Hour, "How old directories must be before being eligible for deletion when empty")
	atimeUpdateThresholdFlag  = flag.Duration("cache.pebble.atime_update_threshold", DefaultAtimeUpdateThreshold, "Don't update atime if it was updated more recently than this")
	atimeBufferSizeFlag       = flag.Int("cache.pebble.atime_buffer_size", DefaultAtimeBufferSize, "Buffer up to this many atime updates in a channel before dropping atime updates")
	minEvictionAgeFlag        = flag.Duration("cache.pebble.min_eviction_age", DefaultMinEvictionAge, "Don't evict anything unless it's been idle for at least this long")
	forceCompaction           = flag.Bool("cache.pebble.force_compaction", false, "If set, compact the DB when it's created")
	forceCalculateMetadata    = flag.Bool("cache.pebble.force_calculate_metadata", false, "If set, partition size and counts will be calculated even if cached information is available.")
	samplesPerEviction        = flag.Int("cache.pebble.samples_per_eviction", 20, "How many records to sample on each eviction")
	deletesPerEviction        = flag.Int("cache.pebble.deletes_per_eviction", 5, "Maximum number keys to delete in one eviction attempt before resampling.")
	samplePoolSize            = flag.Int("cache.pebble.sample_pool_size", 500, "How many deletion candidates to maintain between evictions")
	evictionRateLimit         = flag.Int("cache.pebble.eviction_rate_limit", 300, "Maximum number of entries to evict per second (per partition).")
	copyPartition             = flag.String("cache.pebble.copy_partition_data", "", "If set, all data will be copied from the source partition to the destination partition on startup. The cache will not serve data while the copy is in progress. Specified in format source_partition_id:destination_partition_id,")
	includeMetadataSize       = flag.Bool("cache.pebble.include_metadata_size", false, "If true, include metadata size")

	// ac eviction flags
	acEvictionEnabled       = flag.Bool("cache.pebble.ac_eviction_enabled", false, "Whether AC eviction is enabled.")
	groupIDSamplingEnabled  = flag.Bool("cache.pebble.groupid_sampling_enabled", false, "Whether AC entries are sampled and exported via metrics. Not yet used for eviction.")
	samplesPerGroupID       = flag.Int("cache.pebble.samples_per_groupid", 20, "How many samples to use when approximating AC item count for groups.")
	groupIDSamplesOnStartup = flag.Int("cache.pebble.num_groupid_samples_on_startup", 10, "How many group IDs to sample to on startup.")
	groupIDSampleFrequency  = flag.Duration("cache.pebble_groupid_sample_frequency", 5*time.Second, "How often to perform a new group ID / approximate count sampling.")

	activeKeyVersion  = flag.Int64("cache.pebble.active_key_version", int64(filestore.UndefinedKeyVersion), "The key version new data will be written with")
	migrationQPSLimit = flag.Int("cache.pebble.migration_qps_limit", 50, "QPS limit for data version migration")

	// Compression related flags
	minBytesAutoZstdCompression = flag.Int64("cache.pebble.min_bytes_auto_zstd_compression", 0, "Blobs larger than this will be zstd compressed before written to disk.")

	// Chunking related flags
	averageChunkSizeBytes = flag.Int("cache.pebble.average_chunk_size_bytes", 0, "Average size of chunks that's stored in the cache. Disabled if 0.")
)

var (
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

	DefaultPartitionID           = "default"
	partitionDirectoryPrefix     = "PT"
	partitionMetadataFlushPeriod = 5 * time.Second
	metricsRefreshPeriod         = 30 * time.Second

	// CompressorBufSizeBytes is the buffer size we use for each chunk when compressing data
	// It should be relatively large to get a good compression ratio bc each chunk is compressed independently
	CompressorBufSizeBytes = 4e6 // 4 MB

	// Default values for Options
	DefaultBlockCacheSizeBytes    = int64(1000 * megabyte)
	DefaultMaxInlineFileSizeBytes = int64(1024)

	// Maximum amount of time to wait for a pebble Sync. A warning will be
	// logged if a sync takes longer than this.
	maxSyncDuration = 10 * time.Second
)

type sizeUpdateOp int

const (
	addSizeOp = iota
	deleteSizeOp
)

// Options is a struct containing the pebble cache configuration options.
// Once a cache is created, the options may not be changed.
type Options struct {
	Name              string
	RootDirectory     string
	Partitions        []disk.Partition
	PartitionMappings []disk.PartitionMapping

	MinBytesAutoZstdCompression int64

	MaxSizeBytes           int64
	BlockCacheSizeBytes    int64
	MaxInlineFileSizeBytes int64
	AverageChunkSizeBytes  int

	AtimeUpdateThreshold *time.Duration
	AtimeBufferSize      *int
	MinEvictionAge       *time.Duration

	IncludeMetadataSize bool

	Clock clockwork.Clock

	ClearCacheOnStartup bool
}

type sizeUpdate struct {
	partID    string
	cacheType rspb.CacheType
	delta     int64
}

type accessTimeUpdate struct {
	key filestore.PebbleKey
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
	averageChunkSizeBytes  int

	includeMetadataSize bool

	atimeUpdateThreshold time.Duration
	atimeBufferSize      int
	minEvictionAge       time.Duration

	minDBVersion filestore.PebbleKeyVersion
	maxDBVersion filestore.PebbleKeyVersion
	migrators    []keyMigrator

	env    environment.Env
	db     pebble.IPebbleDB
	leaser pebble.Leaser
	locker lockmap.Locker
	clock  clockwork.Clock

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

	minBytesAutoZstdCompression int64

	oldMetrics    pebble.Metrics
	eventListener *pebbleEventListener
}

type pebbleEventListener struct {
	// Atomicly accessed metrics updated by pebble callbacks.
	writeStallCount      int64
	writeStallDuration   time.Duration
	writeStallStartNanos int64
	diskSlowCount        int64
	diskStallCount       int64
}

func (el *pebbleEventListener) writeStallStats() (int64, time.Duration) {
	count := atomic.LoadInt64(&el.writeStallCount)
	durationInt := atomic.LoadInt64((*int64)(&el.writeStallDuration))
	return count, time.Duration(durationInt)
}

func (el *pebbleEventListener) diskStallStats() (int64, int64) {
	slowCount := atomic.LoadInt64(&el.diskSlowCount)
	stallCount := atomic.LoadInt64(&el.diskStallCount)
	return slowCount, stallCount
}

func (el *pebbleEventListener) WriteStallBegin(info pebble.WriteStallBeginInfo) {
	startNanos := time.Now().UnixNano()
	atomic.StoreInt64(&el.writeStallStartNanos, startNanos)
	atomic.AddInt64(&el.writeStallCount, 1)
}

func (el *pebbleEventListener) WriteStallEnd() {
	startNanos := atomic.SwapInt64(&el.writeStallStartNanos, 0)
	if startNanos == 0 {
		return
	}
	stallDuration := time.Now().UnixNano() - startNanos
	if stallDuration < 0 {
		return
	}
	atomic.AddInt64((*int64)(&el.writeStallDuration), stallDuration)
}

func (el *pebbleEventListener) DiskSlow(info pebble.DiskSlowInfo) {
	if info.Duration.Seconds() >= maxSyncDuration.Seconds() {
		atomic.AddInt64(&el.diskStallCount, 1)
		log.Errorf("Pebble Cache: disk stall: unable to write %q in %.2f seconds.", info.Path, info.Duration.Seconds())
		return
	}
	atomic.AddInt64(&el.diskSlowCount, 1)
}

type keyMigrator interface {
	FromVersion() filestore.PebbleKeyVersion
	ToVersion() filestore.PebbleKeyVersion
	Migrate(val []byte) []byte
}

type v0ToV1Migrator struct{}

func (m *v0ToV1Migrator) FromVersion() filestore.PebbleKeyVersion {
	return filestore.UndefinedKeyVersion
}
func (m *v0ToV1Migrator) ToVersion() filestore.PebbleKeyVersion { return filestore.Version1 }
func (m *v0ToV1Migrator) Migrate(val []byte) []byte             { return val }

type v1ToV2Migrator struct{}

func (m *v1ToV2Migrator) FromVersion() filestore.PebbleKeyVersion {
	return filestore.Version1
}
func (m *v1ToV2Migrator) ToVersion() filestore.PebbleKeyVersion { return filestore.Version2 }
func (m *v1ToV2Migrator) Migrate(val []byte) []byte             { return val }

type v2ToV3Migrator struct{}

func (m *v2ToV3Migrator) FromVersion() filestore.PebbleKeyVersion {
	return filestore.Version2
}
func (m *v2ToV3Migrator) ToVersion() filestore.PebbleKeyVersion { return filestore.Version3 }
func (m *v2ToV3Migrator) Migrate(val []byte) []byte             { return val }

type v3ToV4Migrator struct{}

func (m *v3ToV4Migrator) FromVersion() filestore.PebbleKeyVersion {
	return filestore.Version3
}
func (m *v3ToV4Migrator) ToVersion() filestore.PebbleKeyVersion { return filestore.Version4 }
func (m *v3ToV4Migrator) Migrate(val []byte) []byte             { return val }

// Register creates a new PebbleCache from the configured flags and sets it in
// the provided env.
func Register(env environment.Env) error {
	if *rootDirectoryFlag == "" {
		return nil
	}
	if err := disk.EnsureDirectoryExists(*rootDirectoryFlag); err != nil {
		return err
	}
	opts := &Options{
		Name:                        *nameFlag,
		RootDirectory:               *rootDirectoryFlag,
		Partitions:                  *partitionsFlag,
		PartitionMappings:           *partitionMappingsFlag,
		BlockCacheSizeBytes:         *blockCacheSizeBytesFlag,
		MaxSizeBytes:                cache_config.MaxSizeBytes(),
		MaxInlineFileSizeBytes:      *maxInlineFileSizeBytesFlag,
		MinBytesAutoZstdCompression: *minBytesAutoZstdCompression,
		AtimeUpdateThreshold:        atimeUpdateThresholdFlag,
		AtimeBufferSize:             atimeBufferSizeFlag,
		MinEvictionAge:              minEvictionAgeFlag,
		AverageChunkSizeBytes:       *averageChunkSizeBytes,
		IncludeMetadataSize:         *includeMetadataSize,
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
	if opts.AtimeUpdateThreshold == nil {
		opts.AtimeUpdateThreshold = &DefaultAtimeUpdateThreshold
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
		if part.ID == DefaultPartitionID {
			foundDefaultPartition = true
		}
	}
	if foundDefaultPartition {
		return
	}
	opts.Partitions = append(opts.Partitions, disk.Partition{
		ID:           DefaultPartitionID,
		MaxSizeBytes: opts.MaxSizeBytes,
	})
}

// defaultPebbleOptions returns default pebble config options.
func defaultPebbleOptions(el *pebbleEventListener) *pebble.Options {
	// These values Borrowed from CockroachDB.
	opts := &pebble.Options{
		// The amount of L0 read-amplification necessary to trigger an L0 compaction.
		L0CompactionThreshold:    2,
		MaxConcurrentCompactions: func() int { return 12 },
		MemTableSize:             64 << 20, // 64 MB
		EventListener: pebble.EventListener{
			WriteStallBegin: el.WriteStallBegin,
			WriteStallEnd:   el.WriteStallEnd,
			DiskSlow:        el.DiskSlow,
		},
	}

	// The threshold of L0 read-amplification at which compaction concurrency
	// is enabled (if CompactionDebtConcurrency was not already exceeded).
	// Every multiple of this value enables another concurrent
	// compaction up to MaxConcurrentCompactions.
	opts.Experimental.L0CompactionConcurrency = 4
	// CompactionDebtConcurrency controls the threshold of compaction debt
	// at which additional compaction concurrency slots are added. For every
	// multiple of this value in compaction debt bytes, an additional
	// concurrent compaction is added. This works "on top" of
	// L0CompactionConcurrency, so the higher of the count of compaction
	// concurrency slots as determined by the two options is chosen.
	opts.Experimental.CompactionDebtConcurrency = 10 << 30

	return opts
}

// NewPebbleCache creates a new cache from the provided env and opts.
func NewPebbleCache(env environment.Env, opts *Options) (*PebbleCache, error) {
	SetOptionDefaults(opts)
	if err := validateOpts(opts); err != nil {
		return nil, err
	}
	if opts.ClearCacheOnStartup {
		log.Infof("Removing directory %q before starting cache %s", opts.RootDirectory, opts.Name)
		if err := os.RemoveAll(opts.RootDirectory); err != nil {
			return nil, err
		}
	}
	if err := disk.EnsureDirectoryExists(opts.RootDirectory); err != nil {
		return nil, err
	}
	ensureDefaultPartitionExists(opts)

	el := &pebbleEventListener{}
	pebbleOptions := defaultPebbleOptions(el)
	if opts.BlockCacheSizeBytes > 0 {
		c := pebble.NewCache(opts.BlockCacheSizeBytes)
		defer c.Unref()
		pebbleOptions.Cache = c
	}

	db, err := pebble.Open(opts.RootDirectory, opts.Name, pebbleOptions)
	if err != nil {
		return nil, err
	}
	clock := opts.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}
	pc := &PebbleCache{
		name:                        opts.Name,
		rootDirectory:               opts.RootDirectory,
		partitions:                  opts.Partitions,
		partitionMappings:           opts.PartitionMappings,
		maxSizeBytes:                opts.MaxSizeBytes,
		blockCacheSizeBytes:         opts.BlockCacheSizeBytes,
		maxInlineFileSizeBytes:      opts.MaxInlineFileSizeBytes,
		averageChunkSizeBytes:       opts.AverageChunkSizeBytes,
		atimeUpdateThreshold:        *opts.AtimeUpdateThreshold,
		atimeBufferSize:             *opts.AtimeBufferSize,
		minEvictionAge:              *opts.MinEvictionAge,
		env:                         env,
		db:                          db,
		leaser:                      pebble.NewDBLeaser(db),
		locker:                      lockmap.New(),
		clock:                       clock,
		brokenFilesDone:             make(chan struct{}),
		orphanedFilesDone:           make(chan struct{}),
		eg:                          &errgroup.Group{},
		egSizeUpdates:               &errgroup.Group{},
		statusMu:                    &sync.Mutex{},
		edits:                       make(chan *sizeUpdate, 1000),
		accesses:                    make(chan *accessTimeUpdate, *opts.AtimeBufferSize),
		evictors:                    make([]*partitionEvictor, len(opts.Partitions)),
		fileStorer:                  filestore.New(),
		bufferPool:                  bytebufferpool.New(CompressorBufSizeBytes),
		minBytesAutoZstdCompression: opts.MinBytesAutoZstdCompression,
		eventListener:               el,
		includeMetadataSize:         opts.IncludeMetadataSize,
	}

	versionMetadata, err := pc.databaseVersionMetadata()
	if err != nil {
		return nil, err
	}
	pc.minDBVersion, pc.maxDBVersion = filestore.PebbleKeyVersion(versionMetadata.GetMinVersion()), filestore.PebbleKeyVersion(versionMetadata.GetMaxVersion())
	if pc.activeDatabaseVersion() < pc.minDBVersion {
		pc.minDBVersion = pc.activeDatabaseVersion()
	}
	if pc.activeDatabaseVersion() > pc.maxDBVersion {
		pc.maxDBVersion = pc.activeDatabaseVersion()
	}

	// Update the database version, in case the active version has changed.
	// This will update the DB min/max version, and ensure old/new data is
	// correctly seen and written.
	if err := pc.updateDatabaseVersions(pc.minDBVersion, pc.maxDBVersion); err != nil {
		return nil, err
	}
	log.Infof("Min DB version: %d, Max DB version: %d, Active version: %d", pc.minDBVersion, pc.maxDBVersion, pc.activeDatabaseVersion())

	// Only enable migrators if the data stored in the database lags the
	// currently active version.
	if pc.minDBVersion < pc.activeDatabaseVersion() {
		// N.B. Migrators must be added *in order*.
		if pc.activeDatabaseVersion() >= filestore.Version1 {
			// Migrate keys from 0->1.
			pc.migrators = append(pc.migrators, &v0ToV1Migrator{})
		}
		if pc.activeDatabaseVersion() >= filestore.Version2 {
			// Migrate keys from 1->2.
			pc.migrators = append(pc.migrators, &v1ToV2Migrator{})
		}
		if pc.activeDatabaseVersion() >= filestore.Version3 {
			// Migrate keys from 2->3.
			pc.migrators = append(pc.migrators, &v2ToV3Migrator{})
		}
		if pc.activeDatabaseVersion() >= filestore.Version4 {
			// Migrate keys from 3->4.
			pc.migrators = append(pc.migrators, &v3ToV4Migrator{})
		}
	}

	// Check that there is a migrator enabled to update us to (or past) the
	// activeKeyVersion (flag configured). Warn if not.
	if len(pc.migrators) > 0 {
		lastMigratorVersion := pc.migrators[len(pc.migrators)-1].ToVersion()
		if pc.activeDatabaseVersion() > lastMigratorVersion {
			return nil, status.FailedPreconditionErrorf("Cache versions will never converge! Active key version %d > last migrator version: %d", pc.activeDatabaseVersion(), lastMigratorVersion)
		}
	}

	if *copyPartition != "" {
		partitionIDs := strings.Split(*copyPartition, ":")
		if len(partitionIDs) != 2 {
			return nil, status.InvalidArgumentErrorf("ID specifier %q for partition copy operation invalid", *copyPartition)
		}
		srcPartitionID, dstPartitionID := partitionIDs[0], partitionIDs[1]
		if !hasPartition(opts.Partitions, srcPartitionID) {
			return nil, status.InvalidArgumentErrorf("Copy operation invalid source partition ID %q", srcPartitionID)
		}
		if !hasPartition(opts.Partitions, dstPartitionID) {
			return nil, status.InvalidArgumentErrorf("Copy operation invalid destination partition ID %q", srcPartitionID)
		}
		log.Infof("Copying data from partition %s to partition %s", srcPartitionID, dstPartitionID)
		if err := pc.copyPartitionData(srcPartitionID, dstPartitionID); err != nil {
			return nil, status.UnknownErrorf("could not copy partition data: %s", err)
		}
	}

	peMu := sync.Mutex{}
	eg := errgroup.Group{}
	for i, part := range opts.Partitions {
		i := i
		part := part
		eg.Go(func() error {
			blobDir := pc.blobDir()
			if err := disk.EnsureDirectoryExists(blobDir); err != nil {
				return err
			}
			pe, err := newPartitionEvictor(part, pc.fileStorer, blobDir, pc.leaser, pc.locker, pc, clock, pc.accesses, *opts.AtimeBufferSize, *opts.MinEvictionAge, opts.Name, opts.IncludeMetadataSize)
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

func hasPartition(ps []disk.Partition, id string) bool {
	for _, p := range ps {
		if p.ID == id {
			return true
		}
	}
	return false
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

// databaseVersionKey returns the key bytes of a key where a serialized,
// database-wide version metadata proto is stored.
func (p *PebbleCache) databaseVersionKey() []byte {
	var key []byte
	key = append(key, SystemKeyPrefix...)
	key = append(key, []byte("database-version")...)
	return key
}

// databaseVersionKey returns the database-wide version metadata which
// contains the database version.
func (p *PebbleCache) databaseVersionMetadata() (*rfpb.VersionMetadata, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	buf, err := pebble.GetCopy(db, p.databaseVersionKey())
	if err != nil {
		if status.IsNotFoundError(err) {
			// If the key is not present in the DB; return an empty
			// proto.
			return &rfpb.VersionMetadata{}, nil
		}
		return nil, err
	}

	versionMetadata := &rfpb.VersionMetadata{}
	if err := proto.Unmarshal(buf, versionMetadata); err != nil {
		return nil, err
	}
	return versionMetadata, nil
}

// minDatabaseVersion returns the currently stored filestore.PebbleKeyVersion.
// It is safe to call this function in a loop -- the underlying metadata will
// only be fetched on cache startup and when updated.
func (p *PebbleCache) minDatabaseVersion() filestore.PebbleKeyVersion {
	unlockFn := p.locker.RLock(string(p.databaseVersionKey()))
	defer unlockFn()
	return p.minDBVersion
}

func (p *PebbleCache) maxDatabaseVersion() filestore.PebbleKeyVersion {
	unlockFn := p.locker.RLock(string(p.databaseVersionKey()))
	defer unlockFn()
	return p.maxDBVersion
}

func (p *PebbleCache) activeDatabaseVersion() filestore.PebbleKeyVersion {
	return filestore.PebbleKeyVersion(*activeKeyVersion)
}

// updateDatabaseVersion updates the min and max versions of the database.
// Both the stored metadata and instance variables are updated.
func (p *PebbleCache) updateDatabaseVersions(minVersion, maxVersion filestore.PebbleKeyVersion) error {
	versionKey := p.databaseVersionKey()
	unlockFn := p.locker.Lock(string(versionKey))
	defer unlockFn()

	oldVersionMetadata, err := p.databaseVersionMetadata()
	if err != nil {
		return err
	}

	if oldVersionMetadata.MinVersion == int64(minVersion) && oldVersionMetadata.MaxVersion == int64(maxVersion) {
		log.Debugf("Version metadata already current; not updating!")
		return nil
	}

	newVersionMetadata := proto.Clone(oldVersionMetadata).(*rfpb.VersionMetadata)
	newVersionMetadata.MinVersion = int64(minVersion)
	newVersionMetadata.MaxVersion = int64(maxVersion)
	newVersionMetadata.LastModifyUsec = p.clock.Now().UnixMicro()

	buf, err := proto.Marshal(newVersionMetadata)
	if err != nil {
		return err
	}

	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Set(versionKey, buf, pebble.Sync); err != nil {
		return err
	}

	p.minDBVersion = minVersion
	p.maxDBVersion = maxVersion

	log.Printf("Pebble Cache: db version changed from %+v to %+v", oldVersionMetadata, newVersionMetadata)
	return nil
}

func (p *PebbleCache) updateAtime(key filestore.PebbleKey) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	// Write Lock: because we read/modify/write below.
	unlockFn := p.locker.Lock(key.LockID())
	defer unlockFn()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	md, version, err := p.lookupFileMetadataAndVersion(p.env.GetServerContext(), iter, key)
	if err != nil {
		return err
	}

	atime := time.UnixMicro(md.GetLastAccessUsec())
	if !olderThanThreshold(atime, p.atimeUpdateThreshold) {
		return nil
	}
	md.LastAccessUsec = p.clock.Now().UnixMicro()
	protoBytes, err := proto.Marshal(md)
	if err != nil {
		return err
	}
	keyBytes, err := key.Bytes(version)
	if err != nil {
		return err
	}
	metrics.PebbleCacheAtimeUpdateCount.With(prometheus.Labels{
		metrics.CacheNameLabel: p.name,
		metrics.PartitionID:    md.GetFileRecord().GetIsolation().GetPartitionId(),
	}).Inc()
	return db.Set(keyBytes, protoBytes, pebble.NoSync)
}

func (p *PebbleCache) migrateData(quitChan chan struct{}) error {
	if len(p.migrators) == 0 {
		log.Debugf("No migrations necessary")
		return nil
	}

	limiter := rate.NewLimiter(rate.Limit(*migrationQPSLimit), 1)
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: keys.MinByte,
		UpperBound: keys.MaxByte,
	})
	defer iter.Close()

	minVersion := p.maxDatabaseVersion()
	maxVersion := p.minDatabaseVersion()
	migrationStart := time.Now()
	keysSeen := 0
	keysMigrated := 0
	lastStatusUpdate := time.Now()

	for iter.First(); iter.Valid(); iter.Next() {
		if bytes.HasPrefix(iter.Key(), SystemKeyPrefix) {
			continue
		}
		keysSeen += 1

		select {
		case <-quitChan:
			return nil
		default:
		}

		if time.Since(lastStatusUpdate) > 10*time.Second {
			log.Infof("Pebble Cache: data migration progress: saw %d keys, migrated %d to version: %d in %s. Current key: %q", keysSeen, keysMigrated, maxVersion, time.Since(migrationStart), string(iter.Key()))
			lastStatusUpdate = time.Now()
		}
		var key filestore.PebbleKey
		version, err := key.FromBytes(iter.Key())
		if err != nil {
			return err
		}
		oldVersion := version
		valBytes := iter.Value()

		for _, migrator := range p.migrators {
			// If this key was already migrated, skip this migrator.
			if version >= migrator.ToVersion() {
				continue
			}

			// If this key does not match this migrator's
			// "FromVersion", then the migrators that should have
			// run before this one did not, and something is wrong.
			// Bail out.
			if version != migrator.FromVersion() {
				return status.FailedPreconditionErrorf("Migrator %+v cannot migrate key from version %d", migrator, version)
			}

			valBytes = migrator.Migrate(valBytes)
			version = migrator.ToVersion()
		}
		if version == oldVersion {
			continue
		}
		keysMigrated += 1

		if version > maxVersion {
			maxVersion = version
		}
		if version < minVersion {
			minVersion = version
		}

		_ = limiter.Wait(p.env.GetServerContext())

		moveKey := func() error {
			keyBytes, err := key.Bytes(version)
			if err != nil {
				return err
			}
			// Don't do anything if the key is already gone, it could have been
			// already deleted by eviction.
			_, closer, err := db.Get(iter.Key())
			if err == pebble.ErrNotFound {
				return nil
			}
			if err != nil {
				return status.UnknownErrorf("could not read key to be migrated: %s", err)
			}
			_ = closer.Close()
			if err := db.Set(keyBytes, valBytes, pebble.NoSync); err != nil {
				return status.UnknownErrorf("could not write migrated key: %s", err)
			}
			if err := db.Delete(iter.Key(), pebble.NoSync); err != nil {
				return status.UnknownErrorf("could not write migrated key: %s", err)
			}
			return nil
		}

		unlockFn := p.locker.Lock(key.LockID())
		err = moveKey()
		unlockFn()

		if err != nil {
			return err
		}

	}

	if p.activeDatabaseVersion() < minVersion {
		minVersion = p.activeDatabaseVersion()
	}
	if p.activeDatabaseVersion() > maxVersion {
		maxVersion = p.activeDatabaseVersion()
	}

	log.Infof("Pebble Cache: data migration complete: migrated %d keys to version: %d", keysMigrated, maxVersion)
	return p.updateDatabaseVersions(minVersion, maxVersion)
}

func (p *PebbleCache) processAccessTimeUpdates(quitChan chan struct{}) error {
	for {
		select {
		case accessTimeUpdate := <-p.accesses:
			if err := p.updateAtime(accessTimeUpdate.key); err != nil {
				log.Warningf("Error updating atime: %s", err)
			}
		case <-quitChan:
			// Drain any updates in the queue before exiting.
			for {
				select {
				case u := <-p.accesses:
					if err := p.updateAtime(u.key); err != nil {
						log.Warningf("Error updating atime: %s", err)
					}
				default:
					return nil
				}
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
		e.updateSize(edit.cacheType, edit.delta)
	}
}

func (p *PebbleCache) copyPartitionData(srcPartitionID, dstPartitionID string) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	dstMetadataKey := partitionMetadataKey(dstPartitionID)
	_, closer, err := db.Get(dstMetadataKey)
	if err == nil {
		defer closer.Close()
		log.Infof("Partition metadata key already exists, skipping copy.")
		return nil
	}

	srcKeyPrefix := []byte(partitionDirectoryPrefix + srcPartitionID)
	dstKeyPrefix := []byte(partitionDirectoryPrefix + dstPartitionID)
	start, end := keys.Range(srcKeyPrefix)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	defer iter.Close()

	blobDir := p.blobDir()
	ctx := context.Background()
	numKeysCopied := 0
	lastUpdate := time.Now()
	for iter.First(); iter.Valid(); iter.Next() {
		if bytes.HasPrefix(iter.Key(), SystemKeyPrefix) {
			continue
		}
		dstKey := append(dstKeyPrefix, bytes.TrimPrefix(iter.Key(), srcKeyPrefix)...)

		fileMetadata := &rfpb.FileMetadata{}
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			return status.UnknownErrorf("Error unmarshalling metadata: %s", err)
		}

		dstFileRecord := proto.Clone(fileMetadata.GetFileRecord()).(*rfpb.FileRecord)
		dstFileRecord.GetIsolation().PartitionId = dstPartitionID
		newStorageMD, err := p.fileStorer.LinkOrCopyFile(ctx, fileMetadata.GetStorageMetadata(), dstFileRecord, blobDir, blobDir)
		if err != nil {
			return status.UnknownErrorf("could not copy files: %s", err)
		}
		fileMetadata.StorageMetadata = newStorageMD

		buf, err := proto.Marshal(fileMetadata)
		if err != nil {
			return status.UnknownErrorf("could not marshal destination metadata: %s", err)
		}
		if err := db.Set(dstKey, buf, pebble.NoSync); err != nil {
			return status.UnknownErrorf("could not write destination key: %s", err)
		}
		numKeysCopied++
		if time.Since(lastUpdate) > 10*time.Second {
			log.Infof("Partition copy in progress, copied %d keys, last key: %s", numKeysCopied, string(iter.Key()))
			lastUpdate = time.Now()
		}
	}

	srcMetadataKey := partitionMetadataKey(srcPartitionID)
	v, closer, err := db.Get(srcMetadataKey)
	if err == nil {
		defer closer.Close()
		if err := db.Set(dstMetadataKey, v, pebble.NoSync); err != nil {
			return err
		}
	} else if err != pebble.ErrNotFound {
		return err
	}

	return nil
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

		blobDir := p.blobDir()

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

		var key filestore.PebbleKey
		if _, err := key.FromBytes([]byte(strings.Join(parts, sep))); err != nil {
			return err
		}

		unlockFn := p.locker.RLock(key.LockID())
		_, err = p.lookupFileMetadata(p.env.GetServerContext(), iter, key)
		unlockFn()

		if status.IsNotFoundError(err) {
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
	blobDir := p.blobDir()
	if err := filepath.WalkDir(blobDir, walkFn); err != nil {
		alert.UnexpectedEvent("pebble_cache_error_deleting_orphans", "err: %s", err)
	}
	log.Infof("Pebble Cache: deleteOrphanedFiles removed %d files", orphanCount)
	close(p.orphanedFilesDone)
	return nil
}

func (p *PebbleCache) backgroundRepair(quitChan chan struct{}) error {
	fixMissingFiles := *scanForMissingFiles
	fixOLDACEntries := *deleteACEntriesOlderThan != 0

	for {
		// Nothing to do?
		if !fixMissingFiles && !fixOLDACEntries {
			return nil
		}

		opts := &repairOpts{
			deleteEntriesWithMissingFiles: fixMissingFiles,
			deleteACEntriesOlderThan:      *deleteACEntriesOlderThan,
		}
		err := p.backgroundRepairIteration(quitChan, opts)
		if err != nil {
			log.Warningf("Pebble Cache: backgroundRepairIteration failed: %s", err)
		} else {
			if fixMissingFiles {
				close(p.brokenFilesDone)
				fixMissingFiles = false
			}
		}

		select {
		case <-quitChan:
			return nil
		case <-time.After(*backgroundRepairFrequency):
			break
		}
	}
}

type repairOpts struct {
	deleteEntriesWithMissingFiles bool
	deleteACEntriesOlderThan      time.Duration
}

func (p *PebbleCache) backgroundRepairPartition(db pebble.IPebbleDB, evictor *partitionEvictor, quitChan chan struct{}, opts *repairOpts) {
	partitionID := evictor.part.ID
	log.Infof("Pebble Cache: backgroundRepair starting for partition %q", partitionID)

	keyPrefix := []byte(fmt.Sprintf("%s/%s", evictor.partitionKeyPrefix(), filestore.GroupIDPrefix))
	if opts.deleteEntriesWithMissingFiles {
		keyPrefix = []byte(evictor.partitionKeyPrefix() + "/")
	}
	lowerBound, upperBound := keys.Range(keyPrefix)

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	// We update the iter variable later on, so we need to wrap the Close call
	// in a func to operate on the correct iterator instance.
	defer func() {
		iter.Close()
	}()

	pr := message.NewPrinter(language.English)
	fileMetadata := &rfpb.FileMetadata{}
	blobDir := ""

	modLim := rate.NewLimiter(rate.Limit(*backgroundRepairQPSLimit), 1)
	lastUpdate := time.Now()
	totalCount := 0
	missingFiles := 0
	oldACEntries := 0
	oldACEntriesBytes := int64(0)
	uncompressedCount := 0
	uncompressedBytes := int64(0)
	for iter.First(); iter.Valid(); iter.Next() {
		// Check if we're shutting down; exit if so.
		select {
		case <-quitChan:
			return
		default:
		}

		// Create a new iterator once in a while to avoid holding on to sstables
		// for too long.
		if totalCount != 0 && totalCount%1_000_000 == 0 {
			k := make([]byte, len(iter.Key()))
			copy(k, iter.Key())
			newIter := db.NewIter(&pebble.IterOptions{
				LowerBound: k,
				UpperBound: upperBound,
			})
			iter.Close()
			if !newIter.First() {
				break
			}
			iter = newIter
		}

		if bytes.HasPrefix(iter.Key(), SystemKeyPrefix) {
			continue
		}

		if time.Since(lastUpdate) > 1*time.Minute {
			log.Infof("Pebble Cache: backgroundRepair for %q in progress, scanned %s keys, fixed %d missing files, deleted %s old AC entries consuming %s", partitionID, pr.Sprint(totalCount), missingFiles, pr.Sprint(oldACEntries), units.BytesSize(float64(oldACEntriesBytes)))
			lastUpdate = time.Now()
		}

		totalCount++

		// Attempt a read -- if the file is unreadable; update the metadata.
		keyBytes := iter.Key()
		var key filestore.PebbleKey
		version, err := key.FromBytes(keyBytes)
		if err != nil {
			log.Errorf("Error parsing key: %s", err)
			continue
		}

		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			log.Errorf("Error unmarshaling metadata when scanning for broken files: %s", err)
			continue
		}

		removedEntry := false
		if opts.deleteEntriesWithMissingFiles {
			blobDir = p.blobDir()
			_, err := p.fileStorer.NewReader(p.env.GetServerContext(), blobDir, fileMetadata.GetStorageMetadata(), 0, 0)
			if err != nil {
				_ = modLim.Wait(p.env.GetServerContext())

				unlockFn := p.locker.Lock(key.LockID())
				removed := p.handleMetadataMismatch(p.env.GetServerContext(), err, key, fileMetadata)
				unlockFn()

				if removed {
					missingFiles += 1
					removedEntry = true
				}
			}
		}

		if !removedEntry && opts.deleteACEntriesOlderThan != 0 && bytes.Contains(keyBytes, acDir) {
			atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
			age := time.Since(atime)
			if age > opts.deleteACEntriesOlderThan {
				_ = modLim.Wait(p.env.GetServerContext())
				err := evictor.deleteFile(key, version, fileMetadata.GetStoredSizeBytes(), fileMetadata.GetStorageMetadata())
				if err != nil {
					log.Warningf("Could not delete old AC key %q: %s", key.String(), err)
				} else {
					removedEntry = true
					oldACEntries++
					oldACEntriesBytes += fileMetadata.GetStoredSizeBytes()
				}
			}
		}

		if !removedEntry && fileMetadata.GetFileRecord().GetCompressor() == repb.Compressor_IDENTITY {
			uncompressedCount++
			uncompressedBytes += fileMetadata.GetStoredSizeBytes()
		}
	}
	log.Infof("Pebble Cache: backgroundRepair for %q scanned %s records (%s uncompressed entries remaining using %s bytes [%s])", partitionID, pr.Sprint(totalCount), pr.Sprint(uncompressedCount), pr.Sprint(uncompressedBytes), units.BytesSize(float64(uncompressedBytes)))
	if opts.deleteEntriesWithMissingFiles {
		log.Infof("Pebble Cache: backgroundRepair for %q deleted %d keys with missing files", partitionID, missingFiles)
	}
	if opts.deleteACEntriesOlderThan != 0 {
		log.Infof("Pebble Cache: backgroundRepair for %q deleted %s AC keys older than %s using %s", partitionID, pr.Sprint(oldACEntries), opts.deleteACEntriesOlderThan, units.BytesSize(float64(oldACEntriesBytes)))
	}
	return
}

func (p *PebbleCache) backgroundRepairIteration(quitChan chan struct{}, opts *repairOpts) error {
	log.Infof("Pebble Cache: backgroundRepairIteration starting")

	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	evictors := make([]*partitionEvictor, len(p.evictors))
	p.statusMu.Lock()
	copy(evictors, p.evictors)
	p.statusMu.Unlock()

	for _, e := range evictors {
		p.backgroundRepairPartition(db, e, quitChan, opts)
	}

	log.Infof("Pebble Cache: backgroundRepairIteration finished")

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
	writeStalls, stallDuration := p.eventListener.writeStallStats()
	diskSlows, diskStalls := p.eventListener.diskStallStats()
	buf += fmt.Sprintf("Write stalls: %d, total stall duration: %s\n", writeStalls, stallDuration)
	buf += fmt.Sprintf("Disk slow count: %d, disk stall count: %d\n", diskSlows, diskStalls)

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
	buf += fmt.Sprintf("Min DB version: %d, Max DB version: %d, Active version: %d\n", p.minDatabaseVersion(), p.maxDatabaseVersion(), p.activeDatabaseVersion())
	buf += fmt.Sprintf("[All Partitions] Total Size: %d bytes\n", totalSizeBytes)
	buf += fmt.Sprintf("[All Partitions] CAS total: %d items\n", totalCASCount)
	buf += fmt.Sprintf("[All Partitions] AC total: %d items\n", totalACCount)
	buf += "</pre>"
	for _, e := range evictors {
		buf += e.Statusz(ctx)
	}
	return buf
}

func (p *PebbleCache) userGroupID(ctx context.Context) string {
	auth := p.env.GetAuthenticator()
	if auth == nil {
		return interfaces.AuthAnonymousUser
	}
	user, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return user.GetGroupID()
}

func (p *PebbleCache) lookupGroupAndPartitionID(ctx context.Context, remoteInstanceName string) (string, string) {
	groupID := p.userGroupID(ctx)
	for _, pm := range p.partitionMappings {
		if pm.GroupID == groupID && strings.HasPrefix(remoteInstanceName, pm.Prefix) {
			return groupID, pm.PartitionID
		}
	}
	return groupID, DefaultPartitionID
}

func (p *PebbleCache) encryptionEnabled(ctx context.Context) (bool, error) {
	auth := p.env.GetAuthenticator()
	if auth == nil {
		return false, nil
	}
	u, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return false, nil
	}
	if !u.GetCacheEncryptionEnabled() {
		return false, nil
	}
	if p.env.GetCrypter() == nil {
		return false, status.FailedPreconditionError("encryption requested, but crypter not available")
	}

	return true, nil
}

func (p *PebbleCache) makeFileRecord(ctx context.Context, r *rspb.ResourceName) (*rfpb.FileRecord, error) {
	rn := digest.ResourceNameFromProto(r)
	if err := rn.Validate(); err != nil {
		return nil, err
	}

	groupID, partID := p.lookupGroupAndPartitionID(ctx, rn.GetInstanceName())

	encryptionEnabled, err := p.encryptionEnabled(ctx)
	if err != nil {
		return nil, err
	}

	var encryption *rfpb.Encryption
	if encryptionEnabled {
		ak, err := p.env.GetCrypter().ActiveKey(ctx)
		if err != nil {
			return nil, status.UnavailableErrorf("encryption key not available: %s", err)
		}
		encryption = &rfpb.Encryption{KeyId: ak.GetEncryptionKeyId()}
	}

	return &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:          rn.GetCacheType(),
			RemoteInstanceName: rn.GetInstanceName(),
			PartitionId:        partID,
			GroupId:            groupID,
		},
		Digest:         rn.GetDigest(),
		DigestFunction: rn.GetDigestFunction(),
		Compressor:     rn.GetCompressor(),
		Encryption:     encryption,
	}, nil
}

// blobDir returns a directory path under the root directory where blobs can be stored.
func (p *PebbleCache) blobDir() string {
	filePath := filepath.Join(p.rootDirectory, "blobs")
	return filePath
}

func (p *PebbleCache) lookupFileMetadataAndVersion(ctx context.Context, iter pebble.Iterator, key filestore.PebbleKey) (*rfpb.FileMetadata, filestore.PebbleKeyVersion, error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()

	fileMetadata := &rfpb.FileMetadata{}
	var lastErr error
	for version := p.maxDatabaseVersion(); version >= p.minDatabaseVersion(); version-- {
		keyBytes, err := key.Bytes(version)
		if err != nil {
			return nil, -1, err
		}
		lastErr = pebble.LookupProto(iter, keyBytes, fileMetadata)
		if lastErr == nil {
			return fileMetadata, version, nil
		}
	}
	return nil, -1, lastErr
}

func (p *PebbleCache) lookupFileMetadata(ctx context.Context, iter pebble.Iterator, key filestore.PebbleKey) (*rfpb.FileMetadata, error) {
	md, _, err := p.lookupFileMetadataAndVersion(ctx, iter, key)
	return md, err
}

// getLastAccessUsec processes the FileMetadata as a sequence of (tag,value)
// pairs. It loops through the pairs until hitting the tag for last_acess_usec,
// then it returns the value. This lets us avoid a full parse of FileMetadata.
func getLastAccessUsec(b []byte) int64 {
	// This needs to match the field number for the last_access_usec field in
	// FileMetadata in proto/raft.proto
	const lastAccessUsecFieldNumber = 4
	v, _ := pbwireutil.ConsumeFirstVarint(b, lastAccessUsecFieldNumber)
	return int64(v)
}

// iterHasKey returns a bool indicating if the provided iterator has the
// exact key specified.
func (p *PebbleCache) iterHasKey(iter pebble.Iterator, key filestore.PebbleKey) (bool, error) {
	for version := p.maxDatabaseVersion(); version >= p.minDatabaseVersion(); version-- {
		keyBytes, err := key.Bytes(version)
		if err != nil {
			return false, err
		}
		if iter.SeekGE(keyBytes) && bytes.Equal(iter.Key(), keyBytes) {
			return true, nil
		}
	}
	return false, nil
}

func readFileMetadata(ctx context.Context, reader pebble.Reader, keyBytes []byte) (*rfpb.FileMetadata, error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()

	fileMetadata := &rfpb.FileMetadata{}
	buf, err := pebble.GetCopy(reader, keyBytes)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(buf, fileMetadata); err != nil {
		return nil, err
	}

	return fileMetadata, nil
}

func (p *PebbleCache) handleMetadataMismatch(ctx context.Context, causeErr error, key filestore.PebbleKey, fileMetadata *rfpb.FileMetadata) bool {
	if !status.IsNotFoundError(causeErr) && !os.IsNotExist(causeErr) {
		return false
	}
	if fileMetadata.GetStorageMetadata().GetFileMetadata() != nil {
		err := p.deleteMetadataOnly(ctx, key)
		if err != nil && status.IsNotFoundError(err) {
			return false
		}
		log.Warningf("Metadata record %q was found but file (%+v) not found on disk: %s", key.String(), fileMetadata, causeErr)
		if err != nil {
			log.Warningf("Error deleting metadata: %s", err)
			return false
		}
		return true
	}
	return false
}

func (p *PebbleCache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	missing, err := p.FindMissing(ctx, []*rspb.ResourceName{r})
	if err != nil {
		return false, err
	}
	return len(missing) == 0, nil
}

func (p *PebbleCache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
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
	key, err := p.fileStorer.PebbleKey(fileRecord)
	if err != nil {
		return nil, err
	}

	unlockFn := p.locker.RLock(key.LockID())
	defer unlockFn()

	md, err := p.lookupFileMetadata(ctx, iter, key)
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

func (p *PebbleCache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	var missing []*repb.Digest
	for _, r := range resources {
		err = p.findMissing(ctx, iter, r)
		if err != nil {
			missing = append(missing, r.GetDigest())
		}
	}
	return missing, nil
}

func (p *PebbleCache) findMissing(ctx context.Context, iter pebble.Iterator, r *rspb.ResourceName) error {
	fileRecord, err := p.makeFileRecord(ctx, r)
	if err != nil {
		return err
	}
	key, err := p.fileStorer.PebbleKey(fileRecord)
	if err != nil {
		return err
	}

	unlockFn := p.locker.RLock(key.LockID())
	defer unlockFn()
	md, err := p.lookupFileMetadata(ctx, iter, key)
	if err != nil {
		return err
	}
	chunkedMD := md.GetStorageMetadata().GetChunkedMetadata()
	for _, chunked := range chunkedMD.GetResource() {
		err = p.findMissing(ctx, iter, chunked)
		if err != nil {
			return err
		}
	}
	p.sendAtimeUpdate(key, md.GetLastAccessUsec())
	return nil
}

func (p *PebbleCache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	rc, err := p.Reader(ctx, r, 0, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (p *PebbleCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	foundMap := make(map[*repb.Digest][]byte, len(resources))

	buf := &bytes.Buffer{}
	for _, r := range resources {
		rc, err := p.reader(ctx, db, r, 0, 0)
		if err != nil {
			if status.IsNotFoundError(err) || os.IsNotExist(err) {
				continue
			}
			return nil, err
		}

		_, copyErr := io.Copy(buf, rc)
		closeErr := rc.Close()
		if copyErr != nil {
			log.Warningf("GetMulti encountered error when copying %s: %s", r.GetDigest().GetHash(), copyErr)
			continue
		}
		if closeErr != nil {
			log.Warningf("GetMulti cannot close reader when copying %s: %s", r.GetDigest().GetHash(), closeErr)
			continue
		}
		foundMap[r.GetDigest()] = append([]byte{}, buf.Bytes()...)
		buf.Reset()
	}
	return foundMap, nil
}

func (p *PebbleCache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
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

func (p *PebbleCache) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
	for r, data := range kvs {
		if err := p.Set(ctx, r, data); err != nil {
			return err
		}
	}
	return nil
}

func (p *PebbleCache) sendSizeUpdate(partID string, cacheType rspb.CacheType, op sizeUpdateOp, md *rfpb.FileMetadata, keySize int) {
	delta := md.GetStoredSizeBytes()
	if p.includeMetadataSize {
		delta = getTotalSizeBytes(md) + int64(keySize)
	}

	if op == deleteSizeOp {
		delta = -1 * delta
	}
	up := &sizeUpdate{
		partID:    partID,
		cacheType: cacheType,
		delta:     delta,
	}
	p.edits <- up
}

func (p *PebbleCache) sendAtimeUpdate(key filestore.PebbleKey, lastAccessUsec int64) {
	atime := time.UnixMicro(lastAccessUsec)
	if !olderThanThreshold(atime, p.atimeUpdateThreshold) {
		return
	}

	up := &accessTimeUpdate{key}

	// If the atimeBufferSize is 0, non-blocking writes do not make sense,
	// so in that case just do a regular channel send. Otherwise; use a non-
	// blocking channel send.
	if p.atimeBufferSize == 0 {
		p.accesses <- up
	} else {
		select {
		case p.accesses <- up:
			return
		default:
			log.Warningf("Dropping atime update for %q", key.String())
		}
	}
}

func (p *PebbleCache) deleteMetadataOnly(ctx context.Context, key filestore.PebbleKey) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	// First, lookup the FileMetadata. If it's not found, we don't have the file.
	fileMetadata, version, err := p.lookupFileMetadataAndVersion(ctx, iter, key)
	if err != nil {
		return err
	}

	fileMetadataKey, err := key.Bytes(version)
	if err != nil {
		return err
	}

	if err := db.Delete(fileMetadataKey, pebble.NoSync); err != nil {
		return err
	}
	p.sendSizeUpdate(fileMetadata.GetFileRecord().GetIsolation().GetPartitionId(), key.CacheType(), deleteSizeOp, fileMetadata, len(fileMetadataKey))
	return nil
}

func (p *PebbleCache) deleteFileAndMetadata(ctx context.Context, key filestore.PebbleKey, version filestore.PebbleKeyVersion, md *rfpb.FileMetadata) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	keyBytes, err := key.Bytes(version)
	if err != nil {
		return err
	}

	// N.B. This deletes the file metadata. Because inlined files are stored
	// with their metadata, this means we don't need to delete the metadata
	// again below in the switch statement.
	if err := db.Delete(keyBytes, pebble.NoSync); err != nil {
		return err
	}

	storageMetadata := md.GetStorageMetadata()
	partitionID := md.GetFileRecord().GetIsolation().GetPartitionId()
	switch {
	case storageMetadata.GetFileMetadata() != nil:
		fp := p.fileStorer.FilePath(p.blobDir(), storageMetadata.GetFileMetadata())
		if err := disk.DeleteFile(ctx, fp); err != nil {
			return err
		}
		parentDir := filepath.Dir(fp)
		if err := deleteDirIfEmptyAndOld(parentDir); err != nil {
			log.Debugf("Error deleting dir: %s: %s", parentDir, err)
		}
	case storageMetadata.GetInlineMetadata() != nil:
		// Already deleted; see comment above.
		break
	case storageMetadata.GetChunkedMetadata() != nil:
		break
	default:
		return status.FailedPreconditionErrorf("Unnown storage metadata type: %+v", storageMetadata)
	}

	p.sendSizeUpdate(partitionID, key.CacheType(), deleteSizeOp, md, len(keyBytes))
	return nil
}

func getTotalSizeBytes(md *rfpb.FileMetadata) int64 {
	mdSize := int64(proto.Size(md))
	if md.GetStorageMetadata().GetInlineMetadata() != nil {
		// For inline metadata, the size of the metadata include the stored size
		// bytes.
		return mdSize
	}
	return mdSize + md.GetStoredSizeBytes()
}

func (p *PebbleCache) Delete(ctx context.Context, r *rspb.ResourceName) error {
	fileRecord, err := p.makeFileRecord(ctx, r)
	if err != nil {
		return err
	}
	key, err := p.fileStorer.PebbleKey(fileRecord)
	if err != nil {
		return err
	}

	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	unlockFn := p.locker.Lock(key.LockID())
	defer unlockFn()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	md, err := p.lookupFileMetadata(ctx, iter, key)
	if err != nil {
		return err
	}

	// TODO(tylerw): Make version aware.
	if err := p.deleteFileAndMetadata(ctx, key, filestore.UndefinedKeyVersion, md); err != nil {
		log.Errorf("Error deleting old record %q: %s", key.String(), err)
		return err
	}
	return nil
}

func (p *PebbleCache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rc, err := p.reader(ctx, db, r, uncompressedOffset, limit)
	if err != nil {
		return nil, err
	}

	// Grab another lease and pass the Close function to the reader
	// so it will be closed when the reader is.
	db, err = p.leaser.DB()
	if err != nil {
		return nil, err
	}
	return pebble.ReadCloserWithFunc(rc, db.Close), nil
}

// A writer that will chunk bytes written to it using Content-Defined Chunking,
// and then, if configured, encrypt and compress the chunked bytes.
type cdcWriter struct {
	ctx        context.Context
	pc         *PebbleCache
	fileRecord *rfpb.FileRecord
	key        filestore.PebbleKey

	shouldCompress bool
	isCompressed   bool

	chunker         *chunker.Chunker
	isChunkerClosed bool

	mu            sync.Mutex // protects writtenChunks, numChunks, firstChunk, fileType
	numChunks     int
	firstChunk    []byte
	fileType      rfpb.FileMetadata_FileType
	writtenChunks []*rspb.ResourceName

	eg *errgroup.Group
}

func (p *PebbleCache) newCDCCommitedWriteCloser(ctx context.Context, fileRecord *rfpb.FileRecord, key filestore.PebbleKey, shouldCompress bool, isCompressed bool) (interfaces.CommittedWriteCloser, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(10)
	cdcw := &cdcWriter{
		ctx:            ctx,
		eg:             eg,
		pc:             p,
		key:            key,
		fileRecord:     fileRecord,
		shouldCompress: shouldCompress,
		isCompressed:   isCompressed,
	}
	var wc, decompressor io.WriteCloser
	wc = cdcw

	if isCompressed {
		// If the bytes being written are compressed, we decompress them in
		// order to generate CDC chunks, then compress those chunks.
		decompressor, err = compression.NewZstdDecompressor(cdcw)
		if err != nil {
			return nil, err
		}
		wc = decompressor
	}

	chunker, err := chunker.New(ctx, p.averageChunkSizeBytes, cdcw.writeChunk)
	if err != nil {
		return nil, err
	}
	cdcw.chunker = chunker

	cwc := ioutil.NewCustomCommitWriteCloser(wc)
	cwc.CloseFn = db.Close
	cwc.CommitFn = func(bytesWritten int64) error {
		if decompressor != nil {
			if err := decompressor.Close(); err != nil {
				return status.InternalErrorf("failed to close decompressor: %s", err)
			}
		}

		if err := cdcw.closeChunkerAndWait(); err != nil {
			return status.InternalErrorf("failed to close chunker: %s", err)
		}

		cdcw.mu.Lock()
		defer cdcw.mu.Unlock()

		if cdcw.numChunks == 1 {
			cdcw.fileType = rfpb.FileMetadata_COMPLETE_FILE_TYPE
			// When there is only one single chunk, we want to store the original
			// file record with the original key instead of computed digest from
			// the chunkData. This is because the chunkData can be compressed or
			// encrypted, so the digest computed from it will be different from
			// the original digest.
			return cdcw.writeRawChunk(cdcw.fileRecord, cdcw.key, cdcw.firstChunk)
		}
		now := p.clock.Now().UnixMicro()

		md := &rfpb.FileMetadata{
			FileRecord:      fileRecord,
			StorageMetadata: cdcw.Metadata(),
			// The chunks the file record pointed are stored seperately and are
			// not evicted when this entry is evicted. Therefore, the stored
			// size bytes should be zero to avoid double counting.
			StoredSizeBytes: 0,
			LastAccessUsec:  now,
			LastModifyUsec:  now,
			FileType:        rfpb.FileMetadata_COMPLETE_FILE_TYPE,
		}

		if numChunks := len(md.GetStorageMetadata().GetChunkedMetadata().GetResource()); numChunks <= 1 {
			log.Errorf("expected to have more than one chunks, but actually have %d for digest %s", numChunks, fileRecord.GetDigest().GetHash())
			return status.InternalErrorf("invalid number of chunks (%d)", numChunks)
		}
		return p.writeMetadata(ctx, db, key, md)
	}
	return cwc, nil
}

func (cdcw *cdcWriter) writeChunk(chunkData []byte) error {
	cdcw.mu.Lock()
	defer cdcw.mu.Unlock()

	cdcw.numChunks++

	if cdcw.numChunks == 1 {
		// We will wait to write the first chunk until either cdcw.Commit() is
		// called or the second chunk is encountered.
		// In the former case, there is only one chunk, we don't want to write a
		// file-level metadata entry and a chunk entry into pebble.
		cdcw.firstChunk = make([]byte, len(chunkData))
		copy(cdcw.firstChunk, chunkData)
		return nil
	}

	if cdcw.numChunks == 2 {
		cdcw.fileType = rfpb.FileMetadata_CHUNK_FILE_TYPE
		if err := cdcw.writeChunkWhenMultiple(cdcw.firstChunk); err != nil {
			return err
		}
		// We no longer need the first chunk anymore.
		cdcw.firstChunk = nil
	}
	// we need to copy the data because once the chunker calls Next, chunkData
	// will be invalidated.
	data := make([]byte, len(chunkData))
	copy(data, chunkData)
	return cdcw.writeChunkWhenMultiple(data)
}

func (cdcw *cdcWriter) writeRawChunk(fileRecord *rfpb.FileRecord, key filestore.PebbleKey, chunkData []byte) error {
	ctx := cdcw.ctx
	p := cdcw.pc
	inlineWriter := p.fileStorer.InlineWriter(ctx, fileRecord.GetDigest().GetSizeBytes())
	wcm, err := p.newWrappedWriter(ctx, inlineWriter, fileRecord, key, cdcw.shouldCompress || cdcw.isCompressed, cdcw.fileType)
	if err != nil {
		return err
	}
	defer wcm.Close()
	_, err = wcm.Write(chunkData)
	if err != nil {
		return status.InternalErrorf("failed to write raw chunk: %s", err)
	}
	if err := wcm.Commit(); err != nil {
		return status.InternalErrorf("failed to commit while writing raw chunk: %s", err)
	}
	return nil
}

func (cdcw *cdcWriter) writeChunkWhenMultiple(chunkData []byte) error {
	ctx := cdcw.ctx
	p := cdcw.pc

	d, err := digest.Compute(bytes.NewReader(chunkData), cdcw.fileRecord.GetDigestFunction())
	if err != nil {
		return err
	}

	r := digest.NewResourceName(d, cdcw.fileRecord.GetIsolation().GetRemoteInstanceName(), cdcw.fileRecord.GetIsolation().GetCacheType(), cdcw.fileRecord.GetDigestFunction())
	if cdcw.shouldCompress && cdcw.fileRecord.GetCompressor() == repb.Compressor_IDENTITY {
		// we need to compress the chunk, but this data hasn't been compressed yet.
		// so we need to set the resource name to identity to signal to the nested
		// writer to compress it.
		r.SetCompressor(repb.Compressor_IDENTITY)
	} else {
		r.SetCompressor(repb.Compressor_ZSTD)
	}
	rn := r.ToProto()
	fileRecord, err := p.makeFileRecord(ctx, rn)

	if err != nil {
		return err
	}
	key, err := p.fileStorer.PebbleKey(fileRecord)
	if err != nil {
		return err
	}

	// We use cdcw.writtenChunks for the file-level metadata, and this needs to
	// be in order. Otherwise, when we read the file, the chunks will be
	// read out of order.
	cdcw.writtenChunks = append(cdcw.writtenChunks, rn)

	exists, _ := p.Contains(ctx, rn)

	// We only write the chunk again if it does not exist in the cache. If it
	// exists, we skip the write but the atime will be updated in the Contains
	// call.
	if !exists {
		cdcw.eg.Go(func() error {
			return cdcw.writeRawChunk(fileRecord, key, chunkData)
		})
	}
	return nil
}

func (cdcw *cdcWriter) Write(buf []byte) (int, error) {
	return cdcw.chunker.Write(buf)
}

// closeChunkerAndWait closes the chunker and waiting for the data that has
// already been passed to the chunker to be processed.
func (cdcw *cdcWriter) closeChunkerAndWait() error {
	closeErr := cdcw.chunker.Close()
	cdcw.isChunkerClosed = true
	if err := cdcw.eg.Wait(); err != nil {
		return err
	}
	return closeErr
}

func (cdcw *cdcWriter) Close() error {
	if !cdcw.isChunkerClosed {
		return cdcw.closeChunkerAndWait()
	}

	return nil
}

func (cdcw *cdcWriter) Metadata() *rfpb.StorageMetadata {
	return &rfpb.StorageMetadata{
		ChunkedMetadata: &rfpb.StorageMetadata_ChunkedMetadata{
			Resource: cdcw.writtenChunks,
		},
	}
}

// zstdCompressor compresses bytes before writing them to the nested writer
type zstdCompressor struct {
	cacheName string

	interfaces.CommittedWriteCloser
	compressBuf []byte
	bufferPool  *bytebufferpool.Pool

	numDecompressedBytes int
	numCompressedBytes   int
}

func NewZstdCompressor(cacheName string, wc interfaces.CommittedWriteCloser, bp *bytebufferpool.Pool, digestSize int64) *zstdCompressor {
	compressBuf := bp.Get(digestSize)
	return &zstdCompressor{
		cacheName:            cacheName,
		CommittedWriteCloser: wc,
		compressBuf:          compressBuf,
		bufferPool:           bp,
	}
}

func (z *zstdCompressor) Write(decompressedBytes []byte) (int, error) {
	z.compressBuf = compression.CompressZstd(z.compressBuf, decompressedBytes)
	compressedBytesWritten, err := z.CommittedWriteCloser.Write(z.compressBuf)
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
	return z.CommittedWriteCloser.Close()
}

func (p *PebbleCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// If data is not already compressed, return a writer that will compress it before writing
	// Only compress data over a given size for more optimal compression ratios
	shouldCompress := r.GetCompressor() == repb.Compressor_IDENTITY && r.GetDigest().GetSizeBytes() >= p.minBytesAutoZstdCompression
	isCompressed := r.GetCompressor() == repb.Compressor_ZSTD
	if shouldCompress {
		r = &rspb.ResourceName{
			Digest:         r.GetDigest(),
			DigestFunction: r.GetDigestFunction(),
			InstanceName:   r.GetInstanceName(),
			Compressor:     repb.Compressor_ZSTD,
			CacheType:      r.GetCacheType(),
		}
	}

	fileRecord, err := p.makeFileRecord(ctx, r)
	if err != nil {
		return nil, err
	}
	key, err := p.fileStorer.PebbleKey(fileRecord)
	if err != nil {
		return nil, err
	}

	if p.averageChunkSizeBytes > 0 {
		if r.GetDigest().GetSizeBytes() < int64(p.averageChunkSizeBytes) {
			// Files smaller than averageChunkSizeBytes are highly like to only
			// have one chunk. We write them directly inline to avoid unnecessary
			// processing.
			wcm := p.fileStorer.InlineWriter(ctx, r.GetDigest().GetSizeBytes())
			return p.newWrappedWriter(ctx, wcm, fileRecord, key, shouldCompress, rfpb.FileMetadata_COMPLETE_FILE_TYPE)
		}
		// we enabled cdc chunking
		return p.newCDCCommitedWriteCloser(ctx, fileRecord, key, shouldCompress, isCompressed)
	}

	var wcm interfaces.MetadataWriteCloser
	if r.GetDigest().GetSizeBytes() < p.maxInlineFileSizeBytes {
		wcm = p.fileStorer.InlineWriter(ctx, r.GetDigest().GetSizeBytes())
	} else {
		blobDir := p.blobDir()
		fw, err := p.fileStorer.FileWriter(ctx, blobDir, fileRecord)
		if err != nil {
			return nil, err
		}
		wcm = fw
	}

	return p.newWrappedWriter(ctx, wcm, fileRecord, key, shouldCompress, rfpb.FileMetadata_COMPLETE_FILE_TYPE)
}

// newWrappedWriter returns an interfaces.CommittedWriteCloser that on Write,
// it will
// (1) compress the data if shouldCompress is true; and then
// (2) encrypt the data if encryption is enabled
// (3) write the data using input wcm's Write method.
// On Commit, it will write the metadata for fileRecord.
func (p *PebbleCache) newWrappedWriter(ctx context.Context, wcm interfaces.MetadataWriteCloser, fileRecord *rfpb.FileRecord, key filestore.PebbleKey, shouldCompress bool, fileType rfpb.FileMetadata_FileType) (interfaces.CommittedWriteCloser, error) {
	// Grab another lease and pass the Close function to the writer
	// so it will be closed when the writer is.
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}

	var encryptionMetadata *rfpb.EncryptionMetadata
	cwc := ioutil.NewCustomCommitWriteCloser(wcm)
	cwc.CloseFn = db.Close
	cwc.CommitFn = func(bytesWritten int64) error {
		now := p.clock.Now().UnixMicro()
		md := &rfpb.FileMetadata{
			FileRecord:         fileRecord,
			StorageMetadata:    wcm.Metadata(),
			EncryptionMetadata: encryptionMetadata,
			StoredSizeBytes:    bytesWritten,
			LastAccessUsec:     now,
			LastModifyUsec:     now,
			FileType:           fileType,
		}
		return p.writeMetadata(ctx, db, key, md)
	}

	wc := interfaces.CommittedWriteCloser(cwc)
	shouldEncrypt, err := p.encryptionEnabled(ctx)
	if err != nil {
		_ = wc.Close()
		return nil, err
	}
	if shouldEncrypt {
		ewc, err := p.env.GetCrypter().NewEncryptor(ctx, fileRecord.GetDigest(), wc)
		if err != nil {
			_ = wc.Close()
			return nil, status.UnavailableErrorf("encryptor not available: %s", err)
		}
		encryptionMetadata = ewc.Metadata()
		wc = ewc
	}

	if shouldCompress {
		return NewZstdCompressor(p.name, wc, p.bufferPool, fileRecord.GetDigest().GetSizeBytes()), nil
	}
	return wc, nil
}

func (p *PebbleCache) writeMetadata(ctx context.Context, db pebble.IPebbleDB, key filestore.PebbleKey, md *rfpb.FileMetadata) error {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()

	protoBytes, err := proto.Marshal(md)
	if err != nil {
		return err
	}

	unlockFn := p.locker.Lock(key.LockID())
	defer unlockFn()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	if oldMD, version, err := p.lookupFileMetadataAndVersion(ctx, iter, key); err == nil {
		oldKeyBytes, err := key.Bytes(version)
		if err != nil {
			return err
		}
		if err := db.Delete(oldKeyBytes, pebble.NoSync); err != nil {
			return err
		}
		p.sendSizeUpdate(oldMD.GetFileRecord().GetIsolation().GetPartitionId(), key.CacheType(), deleteSizeOp, oldMD, len(oldKeyBytes))
	}

	keyBytes, err := key.Bytes(p.activeDatabaseVersion())
	if err != nil {
		return err
	}

	if err = db.Set(keyBytes, protoBytes, pebble.NoSync); err == nil {
		partitionID := md.GetFileRecord().GetIsolation().GetPartitionId()
		p.sendSizeUpdate(partitionID, key.CacheType(), addSizeOp, md, len(keyBytes))

		chunkedMD := md.GetStorageMetadata().GetChunkedMetadata()

		sizeBytes := md.GetStoredSizeBytes()
		for _, cm := range chunkedMD.GetResource() {
			// For an entry that points to multiple chunks, the file size is the
			// sum of the size of the chunks instead of stored_size_bytes.
			sizeBytes += cm.GetDigest().GetSizeBytes()
		}
		if md.GetFileType() == rfpb.FileMetadata_COMPLETE_FILE_TYPE {
			metrics.DiskCacheAddedFileSizeBytes.With(prometheus.Labels{metrics.CacheNameLabel: p.name}).Observe(float64(sizeBytes))
			if p.averageChunkSizeBytes != 0 {
				numChunks := 1
				if chunkedMD != nil {
					numChunks = len(chunkedMD.GetResource())
				}
				metrics.PebbleCacheNumChunksPerFile.With(prometheus.Labels{metrics.CacheNameLabel: p.name}).Observe(float64(numChunks))
			}
		}
	}

	return err
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

type evictionKey struct {
	bytes           []byte
	storageMetadata *rfpb.StorageMetadata
}

func (k *evictionKey) ID() string {
	return string(k.bytes)
}

func (k *evictionKey) String() string {
	return string(k.bytes)
}

type groupIDApproxCount struct {
	groupID         string
	count           int64
	cumulativeCount int64
}

type partitionEvictor struct {
	mu            *sync.Mutex
	part          disk.Partition
	fileStorer    filestore.Store
	cacheName     string
	blobDir       string
	dbGetter      pebble.Leaser
	locker        lockmap.Locker
	versionGetter versionGetter
	accesses      chan<- *accessTimeUpdate
	rng           *rand.Rand
	clock         clockwork.Clock

	lru       *approxlru.LRU[*evictionKey]
	sizeBytes int64
	casCount  int64
	acCount   int64

	// Total approximate number of items stored under sampled group IDs.
	// This is the sum of all the counts in the slice below.
	groupIDApproxTotalCount int64

	// List of sampled group IDs and their approximate item counts, sorted by
	// item count.
	groupIDApproxCounts []groupIDApproxCount

	atimeBufferSize int
	minEvictionAge  time.Duration

	includeMetadataSize bool
}

type versionGetter interface {
	minDatabaseVersion() filestore.PebbleKeyVersion
}

func newPartitionEvictor(part disk.Partition, fileStorer filestore.Store, blobDir string, dbg pebble.Leaser, locker lockmap.Locker, vg versionGetter, clock clockwork.Clock, accesses chan<- *accessTimeUpdate, atimeBufferSize int, minEvictionAge time.Duration, cacheName string, includeMetadataSize bool) (*partitionEvictor, error) {
	pe := &partitionEvictor{
		mu:              &sync.Mutex{},
		part:            part,
		fileStorer:      fileStorer,
		blobDir:         blobDir,
		dbGetter:        dbg,
		locker:          locker,
		versionGetter:   vg,
		accesses:        accesses,
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
		clock:           clock,
		atimeBufferSize: atimeBufferSize,
		minEvictionAge:  minEvictionAge,
		cacheName:       cacheName,
	}
	metricLbls := prometheus.Labels{
		metrics.PartitionID:    part.ID,
		metrics.CacheNameLabel: cacheName,
	}
	l, err := approxlru.New(&approxlru.Opts[*evictionKey]{
		SamplePoolSize:              *samplePoolSize,
		SamplesPerEviction:          *samplesPerEviction,
		DeletesPerEviction:          *deletesPerEviction,
		EvictionResampleLatencyUsec: metrics.PebbleCacheEvictionResampleLatencyUsec.With(metricLbls),
		EvictionEvictLatencyUsec:    metrics.PebbleCacheEvictionEvictLatencyUsec.With(metricLbls),
		RateLimit:                   float64(*evictionRateLimit),
		MaxSizeBytes:                int64(JanitorCutoffThreshold * float64(part.MaxSizeBytes)),
		OnEvict: func(ctx context.Context, sample *approxlru.Sample[*evictionKey]) (skip bool, err error) {
			return pe.evict(ctx, sample)
		},
		OnSample: func(ctx context.Context, n int) ([]*approxlru.Sample[*evictionKey], error) {
			return pe.sample(ctx, n)
		},
		OnRefresh: func(ctx context.Context, key *evictionKey) (skip bool, timestamp time.Time, err error) {
			return pe.refresh(ctx, key)
		},
	})
	if err != nil {
		return nil, err
	}
	pe.lru = l

	start := time.Now()
	log.Infof("Pebble Cache: Initializing cache partition %q...", part.ID)
	sizeBytes, casCount, acCount, err := pe.computeSize()
	if err != nil {
		return nil, err
	}
	pe.sizeBytes = sizeBytes
	pe.casCount = casCount
	pe.acCount = acCount
	pe.lru.UpdateSizeBytes(sizeBytes)

	// Sample some random groups at startup so that eviction has something to
	// work with.
	// AC eviction requires version 4 or higher.
	if vg.minDatabaseVersion() >= filestore.Version4 && (*groupIDSamplingEnabled || *acEvictionEnabled) {
		for i := 0; i < *groupIDSamplesOnStartup; i++ {
			pe.updateGroupIDApproxCounts()
		}
	}

	log.Infof("Pebble Cache: Initialized cache partition %q AC: %d, CAS: %d, Size: %d [bytes] in %s", part.ID, pe.acCount, pe.casCount, pe.sizeBytes, time.Since(start))
	return pe, nil
}

func (e *partitionEvictor) sampleGroupID() (string, error) {
	if e.versionGetter.minDatabaseVersion() < filestore.Version4 {
		return "", status.FailedPreconditionErrorf("AC eviction requires at least version 4")
	}
	db, err := e.dbGetter.DB()
	if err != nil {
		return "", err
	}
	defer db.Close()
	start, end := keyRange([]byte(e.partitionKeyPrefix() + "/"))
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	defer iter.Close()

	randomGroupID := fmt.Sprintf("GR%020d", random.RandUint64())
	randomKey := e.partitionKeyPrefix() + "/" + randomGroupID
	if !iter.SeekGE([]byte(randomKey)) {
		return "", status.NotFoundError("did not find key")
	}

	var key filestore.PebbleKey
	if _, err := key.FromBytes(iter.Key()); err != nil {
		return "", err
	}

	// We hit a key without a group ID (i.e. a CAS entry).
	if key.GroupID() == "" {
		return "", status.NotFoundErrorf("did not find key")
	}

	// For sampling purposes, we need to use the real group ID used in the
	// pebble database, not the logical "ANON" group ID.
	if key.GroupID() == interfaces.AuthAnonymousUser {
		return filestore.AnonGroupID, nil
	}

	return key.GroupID(), nil
}

// maximum value of a 32 byte hash, represented as a big int
var maxHashAsBigInt = big.NewInt(0).SetBytes([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

func keyToBigInt(key filestore.PebbleKey) (*big.Int, error) {
	hash, err := hex.DecodeString(key.Hash())
	if err != nil {
		return nil, status.UnknownErrorf("could not parse key %q hash: %s", key.Hash(), err)
	}

	for len(hash) < 32 {
		hash = append(hash, 0)
	}
	if len(hash) > 32 {
		hash = hash[:32]
	}

	return big.NewInt(0).SetBytes(hash), nil
}

// approximates the number of items in a group by measuring the distance
// between adjacent keys over n keys and then taking the median value.
func (e *partitionEvictor) approxGroupItemCount(groupID string) (int64, error) {
	if e.versionGetter.minDatabaseVersion() < filestore.Version4 {
		return 0, status.FailedPreconditionErrorf("AC eviction requires at least version 4")
	}
	db, err := e.dbGetter.DB()
	if err != nil {
		return 0, err
	}
	defer db.Close()

	groupPrefix := []byte(fmt.Sprintf("%s/%s/", e.partitionKeyPrefix(), filestore.FixedWidthGroupID(groupID)))
	lower, upper := keys.Range(groupPrefix)

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	defer iter.Close()

	if !iter.First() {
		return 0, nil
	}

	var approxCounts []int64
	prevKeyAsNum := big.NewInt(0)

	for len(approxCounts) < *samplesPerGroupID {
		var key filestore.PebbleKey
		_, err := key.FromBytes(iter.Key())
		if err != nil {
			return 0, status.UnknownErrorf("could not parse key %q: %s", string(iter.Key()), err)
		}

		keyAsNum, err := keyToBigInt(key)
		if err != nil {
			return 0, err
		}
		if keyAsNum.Cmp(prevKeyAsNum) != 0 {
			gap := big.NewInt(0).Sub(keyAsNum, prevKeyAsNum)
			approxCount := big.NewInt(0).Div(maxHashAsBigInt, gap)
			if !approxCount.IsInt64() {
				log.Warningf("cannot represent %s as an int64", approxCount)
			} else {
				approxCounts = append(approxCounts, approxCount.Int64())
			}
			prevKeyAsNum = keyAsNum
		}

		// If we exhausted all the keys in the group, then we don't need to
		// guess the count since we have the exact count.
		if !iter.Next() {
			return int64(len(approxCounts)), nil
		}
	}

	slices.Sort(approxCounts)
	approxCount := approxCounts[len(approxCounts)/2]
	return approxCount, nil
}

func (e *partitionEvictor) updateMetrics() {
	e.mu.Lock()
	defer e.mu.Unlock()
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

func (e *partitionEvictor) updateSize(cacheType rspb.CacheType, deltaSize int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	deltaCount := int64(1)
	if deltaSize < 0 {
		deltaCount = -1
	}

	switch cacheType {
	case rspb.CacheType_CAS:
		e.casCount += deltaCount
	case rspb.CacheType_AC:
		e.acCount += deltaCount
	case rspb.CacheType_UNKNOWN_CACHE_TYPE:
		log.Errorf("Cannot update cache size: resource of unknown type")
	}
	e.sizeBytes += deltaSize
	e.lru.UpdateSizeBytes(e.sizeBytes)
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

func partitionMetadataKey(partID string) []byte {
	var key []byte
	key = append(key, SystemKeyPrefix...)
	key = append(key, []byte(partID)...)
	key = append(key, []byte("/metadata")...)
	return key
}

func (e *partitionEvictor) lookupPartitionMetadata() (*rfpb.PartitionMetadata, error) {
	db, err := e.dbGetter.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	partitionMDBuf, err := pebble.GetCopy(db, partitionMetadataKey(e.part.ID))
	if err != nil {
		return nil, err
	}

	partitionMD := &rfpb.PartitionMetadata{}
	if err := proto.Unmarshal(partitionMDBuf, partitionMD); err != nil {
		return nil, err
	}
	return partitionMD, nil
}

func (e *partitionEvictor) writePartitionMetadata(db pebble.IPebbleDB, md *rfpb.PartitionMetadata) error {
	bs, err := proto.Marshal(md)
	if err != nil {
		return err
	}
	unlockFn := e.locker.Lock(string(partitionMetadataKey(e.part.ID)))
	defer unlockFn()
	return db.Set(partitionMetadataKey(e.part.ID), bs, pebble.Sync)
}

func (e *partitionEvictor) flushPartitionMetadata(db pebble.IPebbleDB) error {
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
	if err != nil {
		return 0, 0, 0, err
	}

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
	buf += fmt.Sprintf("GC Last run: %s\n", e.lru.LastRun().Format("Jan 02, 2006 15:04:05 MST"))
	lastEvictedStr := "nil"
	if le := e.lru.LastEvicted(); le != nil {
		age := time.Since(le.Timestamp)
		lastEvictedStr = fmt.Sprintf("%q age: %s", string(le.Key.bytes), age)
	}
	buf += fmt.Sprintf("Last evicted item: %s\n", lastEvictedStr)
	buf += "</pre>"
	return buf
}

var digestChars = []byte("abcdef1234567890")

func (e *partitionEvictor) randomKey(digestLength int, groupID string, cacheType rspb.CacheType) ([]byte, error) {
	version := e.versionGetter.minDatabaseVersion()
	buf := bytes.NewBuffer(make([]byte, 0, digestLength))
	for i := 0; i < digestLength; i++ {
		buf.WriteByte(digestChars[e.rng.Intn(len(digestChars))])
	}

	key, err := e.fileStorer.PebbleKey(&rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   cacheType,
			PartitionId: e.part.ID,
			GroupId:     groupID,
		},
		Digest: &repb.Digest{
			Hash: string(buf.Bytes()),
		},
		DigestFunction: repb.DigestFunction_SHA256,
	})
	if err != nil {
		return nil, err
	}
	keyBytes, err := key.Bytes(version)
	if err != nil {
		return nil, err
	}
	return keyBytes, nil
}

func (e *partitionEvictor) evict(ctx context.Context, sample *approxlru.Sample[*evictionKey]) (bool, error) {
	db, err := e.dbGetter.DB()
	if err != nil {
		return false, err
	}
	defer db.Close()

	var key filestore.PebbleKey
	version, err := key.FromBytes(sample.Key.bytes)
	if err != nil {
		return false, err
	}
	unlockFn := e.locker.Lock(key.LockID())
	defer unlockFn()

	_, closer, err := db.Get(sample.Key.bytes)
	if err == pebble.ErrNotFound {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	age := time.Since(sample.Timestamp)
	if err := e.deleteFile(key, version, sample.SizeBytes, sample.Key.storageMetadata); err != nil {
		log.Errorf("Error evicting file for key %q: %s (ignoring)", sample.Key, err)
		return false, nil
	}
	lbls := prometheus.Labels{metrics.PartitionID: e.part.ID, metrics.CacheNameLabel: e.cacheName}
	metrics.DiskCacheNumEvictions.With(lbls).Inc()
	metrics.DiskCacheEvictionAgeMsec.With(lbls).Observe(float64(age.Milliseconds()))
	metrics.DiskCacheLastEvictionAgeUsec.With(lbls).Set(float64(age.Microseconds()))
	return false, nil
}

func (e *partitionEvictor) refresh(ctx context.Context, key *evictionKey) (bool, time.Time, error) {
	db, err := e.dbGetter.DB()
	if err != nil {
		return false, time.Time{}, err
	}
	defer db.Close()

	var pebbleKey filestore.PebbleKey
	if _, err := pebbleKey.FromBytes(key.bytes); err != nil {
		return false, time.Time{}, err
	}
	unlockFn := e.locker.RLock(pebbleKey.LockID())
	defer unlockFn()

	md, err := readFileMetadata(ctx, db, key.bytes)
	if err != nil {
		if !status.IsNotFoundError(err) {
			log.Warningf("could not refresh atime for %q: %s", key.String(), err)
		}
		return true, time.Time{}, nil
	}
	atime := time.UnixMicro(md.GetLastAccessUsec())
	age := e.clock.Since(atime)
	if age < e.minEvictionAge {
		return true, time.Time{}, nil
	}
	return false, atime, nil
}

func (e *partitionEvictor) randomGroupForEvictionSampling() (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.groupIDApproxCounts) == 0 {
		return "", status.NotFoundErrorf("no groups available")
	}
	if e.groupIDApproxTotalCount == 0 {
		return "", status.NotFoundErrorf("no groups available (approx count is zero)")
	}

	n := rand.Int63n(e.groupIDApproxTotalCount)
	pos, _ := slices.BinarySearchFunc(e.groupIDApproxCounts, groupIDApproxCount{cumulativeCount: n}, func(a, b groupIDApproxCount) int {
		if a.cumulativeCount < b.cumulativeCount {
			return -1
		} else if a.cumulativeCount > b.cumulativeCount {
			return 1
		}
		return 0
	})
	if pos == len(e.groupIDApproxCounts) {
		return "", status.NotFoundErrorf("strange, did not find group in list, looking for %d\n%s", n, e.formatGroupIDApproxCounts())
	}
	return e.groupIDApproxCounts[pos].groupID, nil
}

func (e *partitionEvictor) sampleGroup() {
	if !*groupIDSamplingEnabled {
		return
	}

	groupID, err := e.randomGroupForEvictionSampling()
	if status.IsNotFoundError(err) {
		return
	}
	if err != nil {
		log.Warningf("could not sample group in partition %q: %s", e.part.ID, err)
		return
	}
	metricLbls := prometheus.Labels{
		metrics.GroupID:        groupID,
		metrics.CacheNameLabel: e.cacheName,
	}
	metrics.PebbleCacheGroupIDSampleCount.With(metricLbls).Inc()
}

func (e *partitionEvictor) sample(ctx context.Context, k int) ([]*approxlru.Sample[*evictionKey], error) {
	db, err := e.dbGetter.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	start, end := keyRange([]byte(e.partitionKeyPrefix() + "/"))
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	iter.SeekGE(start)
	defer iter.Close()

	if !*acEvictionEnabled {
		e.sampleGroup()
	}

	samples := make([]*approxlru.Sample[*evictionKey], 0, k)

	// generate k random digests and for each:
	//   - seek to the next valid key, and return that file record
	for i := 0; i < k*2; i++ {
		groupID := ""
		cacheType := rspb.CacheType_CAS
		if i%2 == 1 && *acEvictionEnabled {
			gid, err := e.randomGroupForEvictionSampling()
			if err == nil {
				cacheType = rspb.CacheType_AC
				groupID = gid
			} else if !status.IsNotFoundError(err) {
				log.Warningf("no groups to sample for %q: %s", e.part.ID, err)
			}
		}

		randKey, err := e.randomKey(64, groupID, cacheType)
		if err != nil {
			log.Errorf("Error generating random key: %s", err)
			continue
		}
		valid := iter.SeekGE(randKey)
		if !valid {
			continue
		}

		var key filestore.PebbleKey
		if _, err := key.FromBytes(iter.Key()); err != nil {
			return nil, err
		}

		for {
			fileMetadata := &rfpb.FileMetadata{}
			unlockFn := e.locker.RLock(key.LockID())
			err = proto.Unmarshal(iter.Value(), fileMetadata)
			unlockFn()
			if err != nil {
				return nil, err
			}

			atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
			age := e.clock.Since(atime)

			sizeBytes := fileMetadata.GetStoredSizeBytes()
			if e.includeMetadataSize {
				sizeBytes = getTotalSizeBytes(fileMetadata) + int64(len(iter.Key()))
			}

			if age >= e.minEvictionAge {
				keyBytes := make([]byte, len(iter.Key()))
				copy(keyBytes, iter.Key())
				sample := &approxlru.Sample[*evictionKey]{
					Key: &evictionKey{
						bytes:           keyBytes,
						storageMetadata: fileMetadata.GetStorageMetadata(),
					},
					SizeBytes: sizeBytes,
					Timestamp: atime,
				}
				samples = append(samples, sample)
			}

			if !iter.Next() {
				break
			}

			// Check if the next key is for the same digest in which case
			// include it as a possible eviction candidate.
			//
			// This can happen for example if there are multiple AC entries
			// with different "remote instance name hash" values:
			//   PTfoo/GR123/foobar/ac/123
			//   PTfoo/GR123/foobar/ac/456
			// We want to consider both keys for eviction, not just the first
			// one.
			//
			// The same situation can occur after enabling or disabling
			// encryption which can produce multiple keys with the same hash
			// prefix.
			var nextKey filestore.PebbleKey
			if _, err := nextKey.FromBytes(iter.Key()); err != nil {
				return nil, err
			}
			if nextKey.Hash() != key.Hash() {
				break
			}
			key = nextKey
		}
		if len(samples) >= k {
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

func (e *partitionEvictor) deleteFile(key filestore.PebbleKey, version filestore.PebbleKeyVersion, storedSizeBytes int64, storageMetadata *rfpb.StorageMetadata) error {
	keyBytes, err := key.Bytes(version)
	if err != nil {
		return err
	}

	db, err := e.dbGetter.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Delete(keyBytes, pebble.NoSync); err != nil {
		return err
	}

	switch {
	case storageMetadata.GetFileMetadata() != nil:
		fp := e.fileStorer.FilePath(e.blobDir, storageMetadata.GetFileMetadata())
		if err := disk.DeleteFile(context.TODO(), fp); err != nil {
			return err
		}
		parentDir := filepath.Dir(fp)
		if err := deleteDirIfEmptyAndOld(parentDir); err != nil {
			log.Debugf("Error deleting dir: %s: %s", parentDir, err)
		}
	case storageMetadata.GetInlineMetadata() != nil:
		break
	case storageMetadata.GetChunkedMetadata() != nil:
		break
	default:
		return status.FailedPreconditionErrorf("Unnown storage metadata type: %+v", storageMetadata)
	}

	if storedSizeBytes > 0 {
		e.updateSize(key.CacheType(), -1*storedSizeBytes)
	}
	return nil
}

func (e *partitionEvictor) partitionKeyPrefix() string {
	return filestore.PartitionDirectoryPrefix + e.part.ID
}

func (e *partitionEvictor) formatGroupIDApproxCounts() string {
	var sb strings.Builder
	cumulativeCount := int64(0)
	for _, v := range e.groupIDApproxCounts {
		cumulativeCount += v.count
		if cumulativeCount != v.cumulativeCount {
			sb.WriteString(fmt.Sprintf("CUMULATIVE COUNT MISMATCH, expected %d but was %d\n", cumulativeCount, v.cumulativeCount))
		}
		sb.WriteString(fmt.Sprintf("%23s %20d %20d\n", v.groupID, v.count, v.cumulativeCount))
	}
	if cumulativeCount != e.groupIDApproxTotalCount {
		sb.WriteString(fmt.Sprintf("TOTAL COUNT MISMATCH %d vs %d\n", cumulativeCount, e.groupIDApproxTotalCount))
	}
	return sb.String()
}

// samples a random group ID and updates the approximate count for that group
func (e *partitionEvictor) updateGroupIDApproxCounts() {
	randomGroupID, err := e.sampleGroupID()
	if status.IsNotFoundError(err) {
		return
	}
	if err != nil {
		log.Warningf("could not sample group ID: %s", err)
		return
	}

	approxCount, err := e.approxGroupItemCount(randomGroupID)
	if err != nil {
		log.Warningf("could not approximate group count for %s: %s", randomGroupID, err)
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Delete the group if it's already in the list.
	deleteIdx := -1
	for i, w := range e.groupIDApproxCounts {
		if w.groupID == randomGroupID {
			deleteIdx = i
			e.groupIDApproxCounts = append(e.groupIDApproxCounts[:i], e.groupIDApproxCounts[i+1:]...)
			e.groupIDApproxTotalCount -= w.count
			break
		}
	}

	e.groupIDApproxTotalCount += approxCount
	insertIdx, _ := slices.BinarySearchFunc(e.groupIDApproxCounts, groupIDApproxCount{count: approxCount}, func(a, b groupIDApproxCount) int {
		if a.count < b.count {
			return -1
		}
		if a.count > b.count {
			return 1
		}
		return 0
	})
	w := groupIDApproxCount{groupID: randomGroupID, count: approxCount}
	e.groupIDApproxCounts = append(e.groupIDApproxCounts[:insertIdx], append([]groupIDApproxCount{w}, e.groupIDApproxCounts[insertIdx:]...)...)

	// Update cumulative counts starting from the lowest affected index.
	lowestAffectedIdx := insertIdx
	if deleteIdx != -1 && deleteIdx < lowestAffectedIdx {
		lowestAffectedIdx = deleteIdx
	}
	for i := lowestAffectedIdx; i < len(e.groupIDApproxCounts); i++ {
		if i == 0 {
			e.groupIDApproxCounts[i].cumulativeCount = e.groupIDApproxCounts[i].count
		} else {
			e.groupIDApproxCounts[i].cumulativeCount = e.groupIDApproxCounts[i-1].cumulativeCount + e.groupIDApproxCounts[i].count
		}
	}
}

func (e *partitionEvictor) startGroupIDSampler(quitChan chan struct{}) {
	if *activeKeyVersion < int64(filestore.Version4) {
		return
	}

	lastLog := time.Now()

	go func() {
		for {
			select {
			case <-quitChan:
				return
			case <-time.After(*groupIDSampleFrequency):
				if e.versionGetter.minDatabaseVersion() < filestore.Version4 {
					continue
				}
				e.updateGroupIDApproxCounts()

				// temporary -- log the approximate counts so that we can verify
				// tracking is working as intended.
				if time.Since(lastLog) > 10*time.Minute {
					log.Infof("Group ID approx counts:\n%s", e.formatGroupIDApproxCounts())
					lastLog = time.Now()
				}
			}
		}
	}()
}

func (e *partitionEvictor) run(quitChan chan struct{}) error {
	if *groupIDSamplingEnabled || *acEvictionEnabled {
		e.startGroupIDSampler(quitChan)
	}
	e.lru.Start()
	<-quitChan
	e.lru.Stop()
	return nil
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

func (p *PebbleCache) updatePebbleMetrics() error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	m := db.Metrics()
	om := p.oldMetrics

	// Compaction related metrics.
	incCompactionMetric := func(compactionType string, oldValue, newValue int64) {
		lbls := prometheus.Labels{
			metrics.CompactionType: compactionType,
			metrics.CacheNameLabel: p.name,
		}
		metrics.PebbleCachePebbleCompactCount.With(lbls).Add(float64(newValue - oldValue))
	}
	incCompactionMetric("default", om.Compact.DefaultCount, m.Compact.DefaultCount)
	incCompactionMetric("delete_only", om.Compact.DeleteOnlyCount, m.Compact.DeleteOnlyCount)
	incCompactionMetric("elision_only", om.Compact.ElisionOnlyCount, m.Compact.ElisionOnlyCount)
	incCompactionMetric("move", om.Compact.MoveCount, m.Compact.MoveCount)
	incCompactionMetric("read", om.Compact.ReadCount, m.Compact.ReadCount)
	incCompactionMetric("rewrite", om.Compact.RewriteCount, m.Compact.RewriteCount)

	nameLabel := prometheus.Labels{
		metrics.CacheNameLabel: p.name,
	}
	metrics.PebbleCachePebbleCompactEstimatedDebtBytes.With(nameLabel).Set(float64(m.Compact.EstimatedDebt))
	metrics.PebbleCachePebbleCompactInProgressBytes.With(nameLabel).Set(float64(m.Compact.InProgressBytes))
	metrics.PebbleCachePebbleCompactInProgress.With(nameLabel).Set(float64(m.Compact.NumInProgress))
	metrics.PebbleCachePebbleCompactMarkedFiles.With(nameLabel).Set(float64(m.Compact.MarkedFiles))

	// Level metrics.
	for i, l := range m.Levels {
		ol := om.Levels[i]
		lbls := prometheus.Labels{
			metrics.PebbleLevel:    strconv.Itoa(i),
			metrics.CacheNameLabel: p.name,
		}
		metrics.PebbleCachePebbleLevelSublevels.With(lbls).Set(float64(l.Sublevels))
		metrics.PebbleCachePebbleLevelNumFiles.With(lbls).Set(float64(l.NumFiles))
		metrics.PebbleCachePebbleLevelSizeBytes.With(lbls).Set(float64(l.Size))
		metrics.PebbleCachePebbleLevelScore.With(lbls).Set(l.Score)
		metrics.PebbleCachePebbleLevelBytesInCount.With(lbls).Add(float64(l.BytesIn - ol.BytesIn))
		metrics.PebbleCachePebbleLevelBytesIngestedCount.With(lbls).Add(float64(l.BytesIngested - ol.BytesIngested))
		metrics.PebbleCachePebbleLevelBytesMovedCount.With(lbls).Add(float64(l.BytesMoved - ol.BytesMoved))
		metrics.PebbleCachePebbleLevelBytesReadCount.With(lbls).Add(float64(l.BytesRead - ol.BytesRead))
		metrics.PebbleCachePebbleLevelBytesCompactedCount.With(lbls).Add(float64(l.BytesCompacted - ol.BytesCompacted))
		metrics.PebbleCachePebbleLevelBytesFlushedCount.With(lbls).Add(float64(l.BytesFlushed - ol.BytesFlushed))
		metrics.PebbleCachePebbleLevelTablesCompactedCount.With(lbls).Add(float64(l.TablesCompacted - ol.TablesCompacted))
		metrics.PebbleCachePebbleLevelTablesFlushedCount.With(lbls).Add(float64(l.TablesFlushed - ol.TablesFlushed))
		metrics.PebbleCachePebbleLevelTablesIngestedCount.With(lbls).Add(float64(l.TablesIngested - ol.TablesIngested))
		metrics.PebbleCachePebbleLevelTablesMovedCount.With(lbls).Add(float64(l.TablesMoved - ol.TablesMoved))
	}

	// Block cache metrics.
	metrics.PebbleCachePebbleBlockCacheSizeBytes.With(nameLabel).Set(float64(m.BlockCache.Size))

	p.oldMetrics = *m

	return nil
}

func (p *PebbleCache) refreshMetrics(quitChan chan struct{}) {
	evictors := make([]*partitionEvictor, len(p.evictors))
	p.statusMu.Lock()
	copy(evictors, p.evictors)
	p.statusMu.Unlock()
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

			for _, e := range evictors {
				e.updateMetrics()
			}

			if err := p.updatePebbleMetrics(); err != nil {
				log.Warningf("could not update pebble metrics: %s", err)
			}
		}
	}
}

func (p *PebbleCache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	switch compressor {
	case repb.Compressor_IDENTITY, repb.Compressor_ZSTD:
		return true
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

// newChunkedReader returns a reader to read chunked content.
// When shouldDecompress is true, the content read is decompressed.
func (p *PebbleCache) newChunkedReader(ctx context.Context, chunkedMD *rfpb.StorageMetadata_ChunkedMetadata, shouldDecompress bool) (io.ReadCloser, error) {
	missing, err := p.FindMissing(ctx, chunkedMD.GetResource())
	if err != nil {
		return nil, err
	}
	if len(missing) > 0 {
		return nil, status.NotFoundError("chunks were missing")
	}

	pr, pw := io.Pipe()
	go func() {
		for _, resourceName := range chunkedMD.GetResource() {
			rn := proto.Clone(resourceName).(*rspb.ResourceName)
			if shouldDecompress && rn.GetCompressor() == repb.Compressor_ZSTD {
				rn.Compressor = repb.Compressor_IDENTITY
			}
			buf, err := p.Get(ctx, rn)
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if _, err := pw.Write(buf); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		pw.Close()
	}()
	return pr, nil
}

func (p *PebbleCache) reader(ctx context.Context, db pebble.IPebbleDB, r *rspb.ResourceName, uncompressedOffset int64, uncompressedLimit int64) (io.ReadCloser, error) {
	fileRecord, err := p.makeFileRecord(ctx, r)
	if err != nil {
		return nil, err
	}
	key, err := p.fileStorer.PebbleKey(fileRecord)
	if err != nil {
		return nil, err
	}

	unlockFn := p.locker.RLock(key.LockID())
	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()
	fileMetadata, err := p.lookupFileMetadata(ctx, iter, key)
	unlockFn()
	if err != nil {
		return nil, err
	}

	blobDir := p.blobDir()
	requestedCompression := r.GetCompressor()
	cachedCompression := fileMetadata.GetFileRecord().GetCompressor()
	if requestedCompression == cachedCompression &&
		requestedCompression != repb.Compressor_IDENTITY &&
		(uncompressedOffset != 0 || uncompressedLimit != 0) {
		return nil, status.FailedPreconditionError("passthrough compression does not support offset/limit")
	}

	shouldDecrypt := fileMetadata.EncryptionMetadata != nil
	if shouldDecrypt {
		encryptionEnabled, err := p.encryptionEnabled(ctx)
		if err != nil {
			return nil, err
		}
		if !encryptionEnabled {
			return nil, status.NotFoundErrorf("decryption key not available")
		}
	}

	// If the data is stored uncompressed/unencrypted, we can use the offset/limit directly
	// otherwise we need to decompress/decrypt first.
	offset := int64(0)
	limit := int64(0)
	rawStorage := cachedCompression == repb.Compressor_IDENTITY && !shouldDecrypt
	if rawStorage {
		offset = uncompressedOffset
		limit = uncompressedLimit
	}

	shouldDecompress := cachedCompression == repb.Compressor_ZSTD && requestedCompression == repb.Compressor_IDENTITY

	var reader io.ReadCloser
	md := fileMetadata.GetStorageMetadata()
	if chunkedMD := md.GetChunkedMetadata(); chunkedMD != nil {
		reader, err = p.newChunkedReader(ctx, chunkedMD, shouldDecompress)
	} else {
		reader, err = p.fileStorer.NewReader(ctx, blobDir, md, offset, limit)
	}
	if err != nil {
		if status.IsNotFoundError(err) || os.IsNotExist(err) {
			unlockFn := p.locker.Lock(key.LockID())
			p.handleMetadataMismatch(ctx, err, key, fileMetadata)
			unlockFn()
		}
		return nil, err
	}
	p.sendAtimeUpdate(key, fileMetadata.GetLastAccessUsec())

	if !rawStorage {
		if shouldDecrypt {
			d, err := p.env.GetCrypter().NewDecryptor(ctx, fileMetadata.GetFileRecord().GetDigest(), reader, fileMetadata.GetEncryptionMetadata())
			if err != nil {
				return nil, status.UnavailableErrorf("decryptor not available: %s", err)
			}
			reader = d
		}
		if shouldDecompress && md.GetChunkedMetadata() == nil {
			// We don't need to decompress the chunked reader's content since
			// it already returns decompressed content from its children.
			dr, err := compression.NewZstdDecompressingReader(reader)
			if err != nil {
				return nil, err
			}
			reader = dr
		}
		if uncompressedOffset != 0 {
			if _, err := io.CopyN(io.Discard, reader, uncompressedOffset); err != nil {
				_ = reader.Close()
				return nil, err
			}
		}
		if uncompressedLimit != 0 {
			reader = &readCloser{io.LimitReader(reader, uncompressedLimit), reader}
		}
	}

	if requestedCompression == repb.Compressor_ZSTD && cachedCompression == repb.Compressor_IDENTITY {
		bufSize := int64(CompressorBufSizeBytes)
		resourceSize := r.GetDigest().GetSizeBytes()
		if resourceSize > 0 && resourceSize < bufSize {
			bufSize = resourceSize
		}

		readBuf := p.bufferPool.Get(bufSize)
		compressBuf := p.bufferPool.Get(bufSize)

		cr, err := compression.NewZstdCompressingReader(reader, readBuf, compressBuf)
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
	}

	return reader, nil
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
		return p.backgroundRepair(p.quitChan)
	})
	p.eg.Go(func() error {
		err := p.migrateData(p.quitChan)
		if err != nil {
			alert.UnexpectedEvent("pebble_cache_error_migrating_keys", "err: %s", err)
		}
		return err
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

func (p *PebbleCache) SupportsEncryption(ctx context.Context) bool {
	_, partID := p.lookupGroupAndPartitionID(ctx, "")
	for _, part := range p.partitions {
		if part.ID == partID {
			return part.EncryptionSupported
		}
	}
	return false
}
