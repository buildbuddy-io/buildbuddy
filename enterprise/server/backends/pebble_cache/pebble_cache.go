package pebble_cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/chunker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockmap"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/docker/go-units"
	"github.com/elastic/gosigar"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"golang.org/x/time/rate"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

var (
	nameFlag                   = flag.String("cache.pebble.name", DefaultName, "The name used in reporting cache metrics and status.")
	rootDirectoryFlag          = flag.String("cache.pebble.root_directory", "", "The root directory to store the database in.")
	blockCacheSizeBytesFlag    = flag.Int64("cache.pebble.block_cache_size_bytes", DefaultBlockCacheSizeBytes, "How much ram to give the block cache")
	maxInlineFileSizeBytesFlag = flag.Int64("cache.pebble.max_inline_file_size_bytes", DefaultMaxInlineFileSizeBytes, "Files smaller than this may be inlined directly into pebble")
	minGCSFileSizeBytesFlag    = flag.Int64("cache.pebble.min_gcs_file_size_bytes", math.MaxInt64, "Files larger than this may be stored in gcs (0 is disabled).")
	partitionsFlag             = flag.Slice("cache.pebble.partitions", []disk.Partition{}, "")
	partitionMappingsFlag      = flag.Slice("cache.pebble.partition_mappings", []disk.PartitionMapping{}, "")

	backgroundRepairFrequency = flag.Duration("cache.pebble.background_repair_frequency", 1*24*time.Hour, "How frequently to run period background repair tasks.")
	backgroundRepairQPSLimit  = flag.Int("cache.pebble.background_repair_qps_limit", 100, "QPS limit for background repair modifications.")
	scanForMissingFiles       = flag.Bool("cache.pebble.scan_for_missing_files", false, "If set, scan all keys and check if external files are missing on disk. Deletes keys with missing files.")
	scanForOrphanedFiles      = flag.Bool("cache.pebble.scan_for_orphaned_files", false, "If true, scan for orphaned files")
	orphanDeleteDryRun        = flag.Bool("cache.pebble.orphan_delete_dry_run", true, "If set, log orphaned files instead of deleting them")
	dirDeletionDelay          = flag.Duration("cache.pebble.dir_deletion_delay", time.Hour, "How old directories must be before being eligible for deletion when empty")
	atimeUpdateThresholdFlag  = flag.Duration("cache.pebble.atime_update_threshold", DefaultAtimeUpdateThreshold, "Don't update atime if it was updated more recently than this")
	atimeBufferSizeFlag       = flag.Int("cache.pebble.atime_buffer_size", DefaultAtimeBufferSize, "Buffer up to this many atime updates in a channel before dropping atime updates")
	numAtimeUpdateWorkers     = flag.Int("cache.pebble.num_atime_update_workers", DefaultNumAtimeUpdateWorkers, "How many threads to use to update atimes")
	sampleBufferSize          = flag.Int("cache.pebble.sample_buffer_size", DefaultSampleBufferSize, "Buffer up to this many samples for eviction sampling")
	deleteBufferSize          = flag.Int("cache.pebble.delete_buffer_size", DefaultDeleteBufferSize, "Buffer up to this many samples for eviction eviction")
	numDeleteWorkers          = flag.Int("cache.pebble.num_delete_workers", DefaultNumDeleteWorkers, "Number of deletes in parallel")
	samplesPerBatch           = flag.Int("cache.pebble.samples_per_batch", DefaultSamplesPerBatch, "How many keys we read forward every time we get a random key.")
	samplerIterRefreshPeriod  = flag.Duration("cache.pebble.sampler_iter_refresh_peroid", DefaultSamplerIterRefreshPeriod, "How often we refresh iterator in sampler")
	minEvictionAgeFlag        = flag.Duration("cache.pebble.min_eviction_age", DefaultMinEvictionAge, "Don't evict anything unless it's been idle for at least this long")
	forceCompaction           = flag.Bool("cache.pebble.force_compaction", false, "If set, compact the DB when it's created")
	forceCalculateMetadata    = flag.Bool("cache.pebble.force_calculate_metadata", false, "If set, partition size and counts will be calculated even if cached information is available.")
	samplesPerEviction        = flag.Int("cache.pebble.samples_per_eviction", 20, "How many records to sample on each eviction")
	deletesPerEviction        = flag.Int("cache.pebble.deletes_per_eviction", 10, "Maximum number keys to delete in one eviction attempt before resampling.")
	samplePoolSize            = flag.Int("cache.pebble.sample_pool_size", 500, "How many deletion candidates to maintain between evictions")
	evictionRateLimit         = flag.Int("cache.pebble.eviction_rate_limit", 2500, "Maximum number of entries to evict per second (per partition).")
	includeMetadataSize       = flag.Bool("cache.pebble.include_metadata_size", false, "If true, include metadata size")
	enableTableBloomFilter    = flag.Bool("cache.pebble.enable_table_bloom_filter", true, "If true, write bloom filter data with pebble SSTables.")
	enableAutoRatchet         = flag.Bool("cache.pebble.enable_auto_ratchet", false, "If true, automatically upgrade on-disk format to latest version.")
	groupSizeSampleRate       = flag.Float64("cache.pebble.group_size_sample_rate", .01, "Compute estimated size per-group in partitions by sampling at this rate.")
	groupSizeSampleDecayRate  = flag.Float64("cache.pebble.group_size_sample_decay_rate", .95, "Constant factor used to decay away partition size estimates.")

	activeKeyVersion  = flag.Int64("cache.pebble.active_key_version", int64(filestore.UnspecifiedKeyVersion), "The key version new data will be written with. If negative, will write to the highest existing version in the database, or the highest known version if a new database is created.")
	migrationQPSLimit = flag.Int("cache.pebble.migration_qps_limit", 50, "QPS limit for data version migration")

	// Compression related flags
	minBytesAutoZstdCompression = flag.Int64("cache.pebble.min_bytes_auto_zstd_compression", 100, "Blobs larger than this will be zstd compressed before written to disk.")

	// Chunking related flags
	averageChunkSizeBytes = flag.Int("cache.pebble.average_chunk_size_bytes", 0, "Average size of chunks that's stored in the cache. Disabled if 0.")

	// GCS Large File Support
	gcsBucket      = flag.String("cache.pebble.gcs.bucket", "", "The name of the GCS bucket to store build artifact files in.")
	gcsTTLDays     = flag.Int64("cache.pebble.gcs.ttl_days", 0, "An object TTL, specified in days, to apply to the GCS bucket (0 means disabled).")
	gcsCredentials = flag.String("cache.pebble.gcs.credentials", "", "Credentials in JSON format that will be used to authenticate to GCS.", flag.Secret)
	gcsProjectID   = flag.String("cache.pebble.gcs.project_id", "", "The Google Cloud project ID of the project owning the above credentials and GCS bucket.")
	gcsAppName     = flag.String("cache.pebble.gcs.app_name", "", "The app name, under which blobstore data will be stored.")
)

var (
	// Default values for Options
	// (It is valid for these options to be 0, so we use ptrs to indicate whether they're set.
	// Their defaults must be vars so we can take their addresses)
	DefaultAtimeUpdateThreshold     = 10 * time.Minute
	DefaultAtimeBufferSize          = 100000
	DefaultNumAtimeUpdateWorkers    = 16
	DefaultSampleBufferSize         = 8000
	DefaultSamplesPerBatch          = 10000
	DefaultSamplerIterRefreshPeriod = 5 * time.Minute
	DefaultDeleteBufferSize         = 20
	DefaultNumDeleteWorkers         = 16
	DefaultMinEvictionAge           = 6 * time.Hour

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
	JanitorCutoffThreshold = .9

	megabyte = 1e6

	DefaultPartitionID           = "default"
	partitionMetadataFlushPeriod = 5 * time.Second
	metricsRefreshPeriod         = 30 * time.Second

	// CompressorBufSizeBytes is the buffer size we use for each chunk when compressing data
	// It should be relatively large to get a good compression ratio bc each chunk is compressed independently
	CompressorBufSizeBytes = 4e6 // 4 MB

	// Default values for Options
	DefaultBlockCacheSizeBytes    = int64(1000 * megabyte)
	DefaultMaxInlineFileSizeBytes = int64(1024)

	// When a partition's size is lower than the SamplerSleepThreshold, the sampler thread
	// will sleep for SamplerSleepDuration
	SamplerSleepThreshold = float64(0.2)
	SamplerSleepDuration  = 1 * time.Second

	SamplerIterRefreshPeriod = 5 * time.Minute
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

	AtimeUpdateThreshold     *time.Duration
	AtimeBufferSize          *int
	NumAtimeUpdateWorkers    *int
	MinEvictionAge           *time.Duration
	SampleBufferSize         *int
	SamplesPerBatch          *int
	SamplerIterRefreshPeriod *time.Duration
	DeleteBufferSize         *int
	NumDeleteWorkers         *int

	IncludeMetadataSize bool

	ActiveKeyVersion *int64

	Clock clockwork.Clock

	ClearCacheOnStartup bool
	EnableAutoRatchet   bool

	GCSBucket           string
	GCSCredentials      string
	GCSProjectID        string
	GCSAppName          string
	GCSTTLDays          *int64
	MinGCSFileSizeBytes *int64
	FileStorer          filestore.Store
}

type sizeUpdate struct {
	partID         string
	groupID        string
	lastModifyUsec int64
	cacheType      rspb.CacheType
	delta          int64
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

	atimeUpdateThreshold  time.Duration
	atimeBufferSize       int
	numAtimeUpdateWorkers int
	minEvictionAge        time.Duration

	versionMu        *sync.RWMutex // PROTECTS(minDBVersion,maxDBVersion)
	activeKeyVersion int64
	minDBVersion     filestore.PebbleKeyVersion
	maxDBVersion     filestore.PebbleKeyVersion
	migrators        []keyMigrator

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
	bufferPool *bytebufferpool.VariableSizePool

	minBytesAutoZstdCompression int64

	oldMetrics       pebble.Metrics
	metricsCollector *pebble.MetricsCollector

	minGCSFileSizeBytes int64
	gcsTTLDays          int64
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

type v4ToV5Migrator struct{}

func (m *v4ToV5Migrator) FromVersion() filestore.PebbleKeyVersion {
	return filestore.Version4
}
func (m *v4ToV5Migrator) ToVersion() filestore.PebbleKeyVersion { return filestore.Version5 }
func (m *v4ToV5Migrator) Migrate(val []byte) []byte             { return val }

// Register creates a new PebbleCache from the configured flags and sets it in
// the provided env.
func Register(env *real_environment.RealEnv) error {
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
		NumAtimeUpdateWorkers:       numAtimeUpdateWorkers,
		SampleBufferSize:            sampleBufferSize,
		DeleteBufferSize:            deleteBufferSize,
		NumDeleteWorkers:            numDeleteWorkers,
		SamplesPerBatch:             samplesPerBatch,
		SamplerIterRefreshPeriod:    samplerIterRefreshPeriod,
		MinEvictionAge:              minEvictionAgeFlag,
		AverageChunkSizeBytes:       *averageChunkSizeBytes,
		IncludeMetadataSize:         *includeMetadataSize,
		ActiveKeyVersion:            activeKeyVersion,
		GCSBucket:                   *gcsBucket,
		GCSCredentials:              *gcsCredentials,
		GCSProjectID:                *gcsProjectID,
		GCSAppName:                  *gcsAppName,
		GCSTTLDays:                  gcsTTLDays,
		MinGCSFileSizeBytes:         minGCSFileSizeBytesFlag,
		EnableAutoRatchet:           *enableAutoRatchet,
	}
	c, err := NewPebbleCache(env, opts)
	if err != nil {
		return status.InternalErrorf("Error configuring pebble cache: %s", err)
	}
	if *forceCompaction {
		log.Infof("Pebble Cache [%s]: starting manual compaction...", c.name)
		start := time.Now()
		err := c.db.Compact(keys.MinByte, keys.MaxByte, true /*=parallelize*/)
		log.Infof("Pebble Cache [%s]: manual compaction finished in %s", c.name, time.Since(start))
		if err != nil {
			log.Errorf("[%s] Error during compaction: %s", c.name, err)
		}
	}
	c.Start()
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return c.Stop()
	})

	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with pebble cache [%s].", c.name)
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
	if opts.NumAtimeUpdateWorkers == nil {
		opts.NumAtimeUpdateWorkers = &DefaultNumAtimeUpdateWorkers
	}
	if opts.MinEvictionAge == nil {
		opts.MinEvictionAge = &DefaultMinEvictionAge
	}
	if opts.ActiveKeyVersion == nil {
		defaultVersion := int64(filestore.UnspecifiedKeyVersion)
		opts.ActiveKeyVersion = &defaultVersion
	}
	if opts.SampleBufferSize == nil {
		opts.SampleBufferSize = &DefaultSampleBufferSize
	}
	if opts.SamplesPerBatch == nil {
		opts.SamplesPerBatch = &DefaultSamplesPerBatch
	}
	if opts.SamplerIterRefreshPeriod == nil {
		opts.SamplerIterRefreshPeriod = &DefaultSamplerIterRefreshPeriod
	}
	if opts.NumDeleteWorkers == nil {
		opts.NumDeleteWorkers = &DefaultNumDeleteWorkers
	}
	if opts.DeleteBufferSize == nil {
		opts.DeleteBufferSize = &DefaultDeleteBufferSize
	}
	if opts.MinGCSFileSizeBytes == nil || *opts.MinGCSFileSizeBytes == 0 {
		var maxInt int64 = math.MaxInt64
		opts.MinGCSFileSizeBytes = &maxInt
	}
	if opts.GCSTTLDays == nil || *opts.GCSTTLDays == 0 {
		var ttlInDays int64 = 0
		opts.GCSTTLDays = &ttlInDays
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

func warnIfCacheTooLarge(opts *Options) {
	usage, err := disk.GetDirUsage(opts.RootDirectory)
	if err != nil {
		log.Warningf("Error getting usage of pebble root directory %s: %v", opts.RootDirectory, err)
		return
	}
	if opts.MaxSizeBytes > int64(usage.TotalBytes) {
		alert.UnexpectedEvent(fmt.Sprintf("Pebble cache maximum size (%d) exceeds disk partition size (%d)", opts.MaxSizeBytes, usage.TotalBytes))
	}
	var partitionTotal int64
	for _, part := range opts.Partitions {
		if part.MaxSizeBytes <= 0 {
			alert.UnexpectedEvent(fmt.Sprintf("Pebble cache partition %q size (%d) is negative", part.ID, part.MaxSizeBytes))
		}
	}
	if partitionTotal > int64(usage.TotalBytes) {
		alert.UnexpectedEvent(fmt.Sprintf("Pebble cache combined partition size (%d) exceeds maximum size (%d)", partitionTotal, opts.MaxSizeBytes))
	}
}

// defaultPebbleOptions returns default pebble config options.
func defaultPebbleOptions(mc *pebble.MetricsCollector, pcOpts *Options) *pebble.Options {
	// These values Borrowed from CockroachDB.
	opts := &pebble.Options{
		// The amount of L0 read-amplification necessary to trigger an L0 compaction.
		L0CompactionThreshold:    2,
		MaxConcurrentCompactions: func() int { return 18 },
		MemTableSize:             64 << 20, // 64 MB
		MaxOpenFiles:             2000,     // double the default
		EventListener: &pebble.EventListener{
			BackgroundError: mc.BackgroundError,
			WriteStallBegin: mc.WriteStallBegin,
			WriteStallEnd:   mc.WriteStallEnd,
			DiskSlow:        mc.DiskSlow,
			FormatUpgrade: func(fmv pebble.FormatMajorVersion) {
				log.Infof("Pebble Cache: format upgrade to %s", fmv)
			},
		},
	}
	if *enableTableBloomFilter {
		opts.Levels = []pebble.LevelOptions{
			pebble.LevelOptions{
				FilterPolicy: pebble.BloomFilterPolicy(10),
			},
		}
	}
	if pcOpts.EnableAutoRatchet {
		opts.FormatMajorVersion = pebble.FormatNewest
	}

	// The threshold of L0 read-amplification at which compaction concurrency
	// is enabled (if CompactionDebtConcurrency was not already exceeded).
	// Every multiple of this value enables another concurrent
	// compaction up to MaxConcurrentCompactions.
	opts.Experimental.L0CompactionConcurrency = 2
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
	warnIfCacheTooLarge(opts)

	mc := &pebble.MetricsCollector{}
	pebbleOptions := defaultPebbleOptions(mc, opts)
	if opts.BlockCacheSizeBytes > 0 {
		c := pebble.NewCache(opts.BlockCacheSizeBytes)
		defer c.Unref()
		pebbleOptions.Cache = c
	}

	desc, err := pebble.Peek(opts.RootDirectory, pebble.DefaultFS)
	if err != nil {
		return nil, err
	}
	newlyCreated := !desc.Exists
	if !pebbleOptions.ReadOnly && pebbleOptions.FormatMajorVersion > desc.FormatMajorVersion {
		log.Infof("Pebble Cache: upgrading format from %s to %s", desc.FormatMajorVersion, pebbleOptions.FormatMajorVersion)
	}

	db, err := pebble.Open(opts.RootDirectory, opts.Name, pebbleOptions)
	if err != nil {
		return nil, err
	}
	clock := opts.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	fileStorer := opts.FileStorer
	if fileStorer == nil {
		filestoreOpts := make([]filestore.Option, 0)
		if opts.GCSBucket != "" {
			// Create a new GCS Client with compression disabled. This cache
			// will already compress blobs before storing them, so we don't
			// want the gcs lib to attempt to compress them too.
			ctx := env.GetServerContext()
			gcsBlobstore, err := gcs.NewGCSBlobStore(ctx, opts.GCSBucket, "", opts.GCSCredentials, opts.GCSProjectID, false /*=enableCompression*/)
			if err != nil {
				return nil, err
			}
			// Because eviction is critical to how the cache works, if
			// we're unable to ensure the bucket TTL is set correctly we
			// return an error.
			if err := gcsBlobstore.SetBucketCustomTimeTTL(ctx, *opts.GCSTTLDays); err != nil {
				return nil, err
			}
			filestoreOpts = append(filestoreOpts, filestore.WithGCSBlobstore(gcsBlobstore, opts.GCSAppName))
			log.Printf("Pebble Cache: storing files larger than %d bytes in GCS (bucket: %q)", *opts.MinGCSFileSizeBytes, opts.GCSBucket)
			log.Printf("Pebble Cache: GCS TTL is set to %d days", *opts.GCSTTLDays)
		}
		fileStorer = filestore.New(filestoreOpts...)
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
		numAtimeUpdateWorkers:       *opts.NumAtimeUpdateWorkers,
		minEvictionAge:              *opts.MinEvictionAge,
		versionMu:                   &sync.RWMutex{},
		activeKeyVersion:            *opts.ActiveKeyVersion,
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
		bufferPool:                  bytebufferpool.VariableSize(CompressorBufSizeBytes),
		minBytesAutoZstdCompression: opts.MinBytesAutoZstdCompression,
		metricsCollector:            mc,
		includeMetadataSize:         opts.IncludeMetadataSize,
		minGCSFileSizeBytes:         *opts.MinGCSFileSizeBytes,
		gcsTTLDays:                  *opts.GCSTTLDays,
		fileStorer:                  fileStorer,
	}

	versionMetadata, err := pc.DatabaseVersionMetadata()
	if err != nil {
		return nil, err
	}
	if newlyCreated {
		activeVersion := *opts.ActiveKeyVersion
		if activeVersion < 0 {
			activeVersion = int64(filestore.MaxKeyVersion) - 1
		}
		versionMetadata.MinVersion = activeVersion
		versionMetadata.MaxVersion = activeVersion
		versionMetadata.LastModifyUsec = clock.Now().UnixMicro()
	}

	if *opts.ActiveKeyVersion < 0 {
		pc.activeKeyVersion = int64(versionMetadata.MaxVersion)
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
	log.Infof("[%s] Min DB version: %d, Max DB version: %d, Active version: %d", pc.name, pc.minDBVersion, pc.maxDBVersion, pc.activeDatabaseVersion())

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
		if pc.activeDatabaseVersion() >= filestore.Version5 {
			// Migrate keys from 4->5.
			pc.migrators = append(pc.migrators, &v4ToV5Migrator{})
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
			pe, err := newPartitionEvictor(env.GetServerContext(), part, pc.fileStorer, blobDir, pc.leaser, pc.locker, pc, clock, *opts.MinEvictionAge, opts.Name, opts.IncludeMetadataSize, *opts.SampleBufferSize, *opts.SamplesPerBatch, *opts.SamplerIterRefreshPeriod, *opts.DeleteBufferSize, *opts.NumDeleteWorkers)
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

var _dbVersionKey = append(SystemKeyPrefix[:], []byte("database-version")...)

// databaseVersionKey returns the key bytes of a key where a serialized,
// database-wide version metadata proto is stored.
func (p *PebbleCache) databaseVersionKey() []byte {
	return _dbVersionKey
}

// databaseVersionKey returns the database-wide version metadata which
// contains the database version.
func (p *PebbleCache) DatabaseVersionMetadata() (*sgpb.VersionMetadata, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	versionMetadata := &sgpb.VersionMetadata{}
	err = pebble.GetProto(db, p.databaseVersionKey(), versionMetadata)
	if err != nil {
		if status.IsNotFoundError(err) {
			// If the key is not present in the DB; return an empty proto.
			return &sgpb.VersionMetadata{}, nil
		}
		return nil, err
	}

	return versionMetadata, nil
}

// minDatabaseVersion returns the currently stored filestore.PebbleKeyVersion.
// It is safe to call this function in a loop -- the underlying metadata will
// only be fetched on cache startup and when updated.
func (p *PebbleCache) minDatabaseVersion() filestore.PebbleKeyVersion {
	p.versionMu.RLock()
	defer p.versionMu.RUnlock()
	return p.minDBVersion
}

func (p *PebbleCache) minAndMaxDatabaseVersions() (min, max filestore.PebbleKeyVersion) {
	p.versionMu.RLock()
	defer p.versionMu.RUnlock()
	return p.minDBVersion, p.maxDBVersion
}

func (p *PebbleCache) activeDatabaseVersion() filestore.PebbleKeyVersion {
	return filestore.PebbleKeyVersion(p.activeKeyVersion)
}

// updateDatabaseVersion updates the min and max versions of the database.
// Both the stored metadata and instance variables are updated.
func (p *PebbleCache) updateDatabaseVersions(minVersion, maxVersion filestore.PebbleKeyVersion) error {
	p.versionMu.Lock()
	defer p.versionMu.Unlock()

	oldVersionMetadata, err := p.DatabaseVersionMetadata()
	if err != nil {
		return err
	}

	if oldVersionMetadata.MinVersion == int64(minVersion) && oldVersionMetadata.MaxVersion == int64(maxVersion) {
		log.Debugf("Version metadata already current; not updating!")
		return nil
	}

	newVersionMetadata := oldVersionMetadata.CloneVT()
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

	if err := db.Set(p.databaseVersionKey(), buf, pebble.Sync); err != nil {
		return err
	}

	p.minDBVersion = minVersion
	p.maxDBVersion = maxVersion

	log.Printf("Pebble Cache [%s]: db version changed from %+v to %+v", p.name, oldVersionMetadata, newVersionMetadata)
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

	md := sgpb.FileMetadataFromVTPool()
	defer md.ReturnToVTPool()
	version, err := p.lookupFileMetadataAndVersion(p.env.GetServerContext(), db, key, md)
	if err != nil {
		return err
	}
	keyBytes, err := key.Bytes(version)
	if err != nil {
		return err
	}

	atime := time.UnixMicro(md.GetLastAccessUsec())

	if !olderThanThreshold(atime, p.atimeUpdateThreshold) {
		return nil
	}

	newAtime := p.clock.Now()

	if atime.After(newAtime) {
		// Atime updates are queued -- if an object was overwritten
		// before the atime update is processed, and has a later
		// atime, don't attempt to update it.
		return nil
	}

	lbls := prometheus.Labels{
		metrics.PartitionID:    md.GetFileRecord().GetIsolation().GetPartitionId(),
		metrics.CacheNameLabel: p.name,
	}

	// If this is a GCS object, update the custom time and record the new
	// custom time.
	if gcsMetadata := md.GetStorageMetadata().GetGcsMetadata(); gcsMetadata != nil {
		if p.gcsObjectIsPastTTL(gcsMetadata) {
			return nil
		}
		if err := p.fileStorer.UpdateBlobAtime(p.env.GetServerContext(), gcsMetadata, newAtime); err != nil {
			metrics.PebbleCacheAtimeUpdateGCSErrorCount.With(lbls).Inc()
			log.Errorf("Error updating GCS custom time (%q): %s", key, err)
			return err
		}
		md.StorageMetadata.GcsMetadata.LastCustomTimeUsec = newAtime.UnixMicro()
	}

	md.LastAccessUsec = newAtime.UnixMicro()
	protoBytes, err := proto.Marshal(md)
	if err != nil {
		return err
	}
	metrics.PebbleCacheAtimeUpdateCount.With(lbls).Inc()
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

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: keys.MinByte,
		UpperBound: keys.MaxByte,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	minVersion, maxVersion := p.minAndMaxDatabaseVersions()
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
			log.Infof("Pebble Cache [%s]: data migration progress: saw %d keys, migrated %d to version: %d in %s. Current key: %q", p.name, keysSeen, keysMigrated, maxVersion, time.Since(migrationStart), string(iter.Key()))
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

			_ = limiter.Wait(p.env.GetServerContext())

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

	log.Infof("Pebble Cache [%s]: data migration complete: migrated %d keys to version: %d", p.name, keysMigrated, maxVersion)
	return p.updateDatabaseVersions(minVersion, maxVersion)
}

func (p *PebbleCache) processAccessTimeUpdates(quitChan chan struct{}) error {
	for {
		select {
		case accessTimeUpdate := <-p.accesses:
			if err := p.updateAtime(accessTimeUpdate.key); err != nil {
				log.Warningf("[%s] Error updating atime: %s", p.name, err)
			}
		case <-quitChan:
			// Drain any updates in the queue before exiting.
			for {
				select {
				case u := <-p.accesses:
					if err := p.updateAtime(u.key); err != nil {
						log.Warningf("[%s] Error updating atime: %s", p.name, err)
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
		e.updateSize(edit.cacheType, edit.lastModifyUsec, edit.groupID, edit.delta)
	}
}

func (p *PebbleCache) deleteOrphanedFiles(quitChan chan struct{}) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	const sep = "/"
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: keys.MinByte,
		UpperBound: keys.MaxByte,
	})
	if err != nil {
		return err
	}
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
			log.Warningf("[%s] Skipping orphaned file: %q", p.name, path)
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
		md := sgpb.FileMetadataFromVTPool()
		err = p.lookupFileMetadata(p.env.GetServerContext(), db, key, md)
		md.ReturnToVTPool()
		unlockFn()

		if status.IsNotFoundError(err) {
			if *orphanDeleteDryRun {
				fi, err := d.Info()
				if err != nil {
					return err
				}
				log.Infof("[%s] Would delete orphaned file: %s (last modified: %s) which is not in cache", p.name, path, fi.ModTime())
			} else {
				if err := os.Remove(path); err == nil {
					log.Infof("[%s] Removed orphaned file: %q", p.name, path)
				}
			}
			orphanCount += 1
		}

		if orphanCount%1000 == 0 && orphanCount != 0 {
			log.Infof("[%s] Removed %d orphans", p.name, orphanCount)
		}
		return nil
	}
	blobDir := p.blobDir()
	if err := filepath.WalkDir(blobDir, walkFn); err != nil {
		alert.UnexpectedEvent("pebble_cache_error_deleting_orphans", "err [%s]: %s", p.name, err)
	}
	log.Infof("Pebble Cache [%s]: deleteOrphanedFiles removed %d files", p.name, orphanCount)
	close(p.orphanedFilesDone)
	return nil
}

func (p *PebbleCache) backgroundRepair(quitChan chan struct{}) error {
	fixMissingFiles := *scanForMissingFiles

	for {
		// Nothing to do?
		if !fixMissingFiles {
			return nil
		}

		opts := &repairOpts{
			deleteEntriesWithMissingFiles: fixMissingFiles,
		}
		err := p.backgroundRepairIteration(quitChan, opts)
		if err != nil {
			log.Warningf("Pebble Cache [%s]: backgroundRepairIteration failed: %s", p.name, err)
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
			// continue with for loop
		}
	}
}

type repairOpts struct {
	deleteEntriesWithMissingFiles bool
}

func (p *PebbleCache) backgroundRepairPartition(db pebble.IPebbleDB, evictor *partitionEvictor, quitChan chan struct{}, opts *repairOpts) {
	partitionID := evictor.part.ID
	log.Infof("Pebble Cache [%s]: backgroundRepair starting for partition %q", p.name, partitionID)

	keyPrefix := []byte(fmt.Sprintf("%s/%s", evictor.partitionKeyPrefix(), filestore.GroupIDPrefix))
	if opts.deleteEntriesWithMissingFiles {
		keyPrefix = []byte(evictor.partitionKeyPrefix() + "/")
	}
	lowerBound, upperBound := keys.Range(keyPrefix)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		log.Errorf("Error creating pebble iter: %s", err)
		return
	}
	// We update the iter variable later on, so we need to wrap the Close call
	// in a func to operate on the correct iterator instance.
	defer func() {
		iter.Close()
	}()

	pr := message.NewPrinter(language.English)
	fileMetadata := sgpb.FileMetadataFromVTPool()
	defer fileMetadata.ReturnToVTPool()
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
			newIter, err := db.NewIter(&pebble.IterOptions{
				LowerBound: k,
				UpperBound: upperBound,
			})
			if err != nil {
				break
			}
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
			log.Infof("Pebble Cache [%s]: backgroundRepair for %q in progress, scanned %s keys, fixed %d missing files, deleted %s old AC entries consuming %s", p.name, partitionID, pr.Sprint(totalCount), missingFiles, pr.Sprint(oldACEntries), units.BytesSize(float64(oldACEntriesBytes)))
			lastUpdate = time.Now()
		}

		totalCount++

		// Attempt a read -- if the file is unreadable; update the metadata.
		keyBytes := iter.Key()
		var key filestore.PebbleKey
		_, err := key.FromBytes(keyBytes)
		if err != nil {
			log.Errorf("[%s] Error parsing key: %s", p.name, err)
			continue
		}

		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			log.Errorf("[%s] Error unmarshaling metadata when scanning for broken files: %s", p.name, err)
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

		if !removedEntry && fileMetadata.GetFileRecord().GetCompressor() == repb.Compressor_IDENTITY {
			uncompressedCount++
			uncompressedBytes += fileMetadata.GetStoredSizeBytes()
		}

		fileMetadata.ResetVT()
	}
	log.Infof("Pebble Cache [%s]: backgroundRepair for %q scanned %s records (%s uncompressed entries remaining using %s bytes [%s])", p.name, partitionID, pr.Sprint(totalCount), pr.Sprint(uncompressedCount), pr.Sprint(uncompressedBytes), units.BytesSize(float64(uncompressedBytes)))
	if opts.deleteEntriesWithMissingFiles {
		log.Infof("Pebble Cache [%s]: backgroundRepair for %q deleted %d keys with missing files", p.name, partitionID, missingFiles)
	}
}

func (p *PebbleCache) backgroundRepairIteration(quitChan chan struct{}, opts *repairOpts) error {
	log.Infof("Pebble Cache [%s]: backgroundRepairIteration starting", p.name)

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

	log.Infof("Pebble Cache [%s]: backgroundRepairIteration finished", p.name)

	return nil
}

func computeEstimatedSizeByGroup(sizeByGroup map[string]int64, totalSizeBytes int64) map[string]int64 {
	out := make(map[string]int64, len(sizeByGroup))
	totalSampledSize := int64(0)
	for _, s := range sizeByGroup {
		totalSampledSize += s
	}
	totalSampledSize = max(1, totalSampledSize)
	sampleScalingFactor := float64(totalSizeBytes) / float64(totalSampledSize)
	for k, v := range sizeByGroup {
		out[k] = int64(float64(v) * sampleScalingFactor)
	}
	return out
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
	writeStalls, stallDuration := p.metricsCollector.WriteStallStats()
	diskSlows, diskStalls := p.metricsCollector.DiskStallStats()
	buf += fmt.Sprintf("Write stalls: %d, total stall duration: %s\n", writeStalls, stallDuration)
	buf += fmt.Sprintf("Disk slow count: %d, disk stall count: %d\n", diskSlows, diskStalls)

	diskEstimateBytes, err := db.EstimateDiskUsage(keys.MinByte, keys.MaxByte)
	if err == nil {
		buf += fmt.Sprintf("Estimated pebble DB disk usage: %d bytes\n", diskEstimateBytes)
	}
	var totalSizeBytes, totalCASCount, totalACCount, totalSampledSize int64
	sampledSizeByGroup := make(map[string]int64)
	for _, e := range evictors {
		sizeBytes, sbg, casCount, acCount := e.Counts()
		for g, s := range sbg {
			sampledSizeByGroup[g] += s
			totalSampledSize += s
		}
		totalSizeBytes += sizeBytes
		totalCASCount += casCount
		totalACCount += acCount
	}
	estimatedSizeByGroup := computeEstimatedSizeByGroup(sampledSizeByGroup, totalSizeBytes)
	minVersion, maxVersion := p.minAndMaxDatabaseVersions()
	buf += fmt.Sprintf("Min DB version: %d, Max DB version: %d, Active version: %d\n", minVersion, maxVersion, p.activeDatabaseVersion())
	buf += fmt.Sprintf("[All Partitions] Total Size: %d bytes\n", totalSizeBytes)
	buf += fmt.Sprintf("[All Partitions] CAS total: %d items\n", totalCASCount)
	buf += fmt.Sprintf("[All Partitions] AC total: %d items\n", totalACCount)
	if len(estimatedSizeByGroup) > 0 {
		sortedKeys := slices.Sorted(maps.Keys(estimatedSizeByGroup))
		buf += "\n[All partitions] Approximate size by group:\n"
		for _, g := range sortedKeys {
			if s, ok := estimatedSizeByGroup[g]; ok {
				buf += fmt.Sprintf("  %s: %d bytes\n", g, s)
			}
		}
		buf += fmt.Sprintf("  Total size of sampled files: %d bytes\n", totalSampledSize)
	}

	buf += "</pre>"
	for _, e := range evictors {
		buf += e.Statusz(ctx)
	}
	return buf
}

func (p *PebbleCache) userGroupID(ctx context.Context) string {
	user, err := p.env.GetAuthenticator().AuthenticatedUser(ctx)
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
	if !authutil.EncryptionEnabled(ctx, p.env.GetAuthenticator()) {
		return false, nil
	}
	if p.env.GetCrypter() == nil {
		return false, status.FailedPreconditionError("encryption requested, but crypter not available")
	}

	return true, nil
}

func (p *PebbleCache) makeFileRecord(ctx context.Context, r *rspb.ResourceName) (*sgpb.FileRecord, error) {
	rn := digest.ResourceNameFromProto(r)
	if err := rn.Validate(); err != nil {
		return nil, err
	}

	groupID, partID := p.lookupGroupAndPartitionID(ctx, rn.GetInstanceName())

	encryptionEnabled, err := p.encryptionEnabled(ctx)
	if err != nil {
		return nil, err
	}

	var encryption *sgpb.Encryption
	if encryptionEnabled {
		ak, err := p.env.GetCrypter().ActiveKey(ctx)
		if err != nil {
			return nil, status.UnavailableErrorf("encryption key not available: %s", err)
		}
		encryption = &sgpb.Encryption{KeyId: ak.GetEncryptionKeyId()}
	}

	return &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
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

func (p *PebbleCache) lookupFileMetadataAndVersion(ctx context.Context, db pebble.IPebbleDB, key filestore.PebbleKey, fileMetadata *sgpb.FileMetadata) (filestore.PebbleKeyVersion, error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()

	var lastErr error
	for minVersion, version := p.minAndMaxDatabaseVersions(); version >= minVersion; version-- {
		keyBytes, err := key.Bytes(version)
		if err != nil {
			return -1, err
		}
		lastErr = pebble.GetProto(db, keyBytes, fileMetadata)
		if lastErr == nil {
			return version, nil
		}
		fileMetadata.ResetVT()
	}
	return -1, lastErr
}

func (p *PebbleCache) lookupFileMetadata(ctx context.Context, db pebble.IPebbleDB, key filestore.PebbleKey, fileMetadata *sgpb.FileMetadata) error {
	_, err := p.lookupFileMetadataAndVersion(ctx, db, key, fileMetadata)
	return err
}

func (p *PebbleCache) handleMetadataMismatch(ctx context.Context, causeErr error, key filestore.PebbleKey, fileMetadata *sgpb.FileMetadata) bool {
	if !status.IsNotFoundError(causeErr) && !os.IsNotExist(causeErr) {
		return false
	}
	if fileMetadata.GetStorageMetadata().GetFileMetadata() != nil {
		err := p.deleteMetadataOnly(ctx, key)
		if err != nil && status.IsNotFoundError(err) {
			return false
		}
		log.Warningf("[%s] Metadata record %q was found but file (%+v) not found on disk: %s", p.name, key.String(), fileMetadata, causeErr)
		if err != nil {
			log.Warningf("[%s] Error deleting metadata: %s", p.name, err)
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

	md := sgpb.FileMetadataFromVTPool()
	defer md.ReturnToVTPool()
	err = p.lookupFileMetadata(ctx, db, key, md)
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
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	metrics.PebbleCacheFindMissingDigestCount.With(prometheus.Labels{metrics.CacheNameLabel: p.name}).Add(float64(len(resources)))

	var missing []*repb.Digest
	for _, r := range resources {
		err = p.findMissing(ctx, db, r)
		if err != nil {
			missing = append(missing, r.GetDigest())
		}
	}
	return missing, nil
}

func (p *PebbleCache) findMissing(ctx context.Context, db pebble.IPebbleDB, r *rspb.ResourceName) error {
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

	md := sgpb.FileMetadataFromVTPool()
	defer md.ReturnToVTPool()
	err = p.lookupFileMetadata(ctx, db, key, md)
	if err != nil {
		return err
	}

	chunkedMD := md.GetStorageMetadata().GetChunkedMetadata()
	for _, chunked := range chunkedMD.GetResource() {
		err = p.findMissing(ctx, db, chunked)
		if err != nil {
			return err
		}
	}

	// If this object was not chunked, and is somehow stored as a zero-
	// length file, pretend it does not exist.
	if chunkedMD == nil && md.GetStoredSizeBytes() == 0 {
		log.Infof("Ignoring zero-length file. Key: %q, md: %+v", key, md)
		return status.NotFoundError("object not found (zero-length)")
	}
	// If this is a GCS object, ensure the custom time is relatively recent
	// so that we avoid saying something exists when it's been deleted by
	// a GCS lifecycle rule.
	if gcsMetadata := md.GetStorageMetadata().GetGcsMetadata(); gcsMetadata != nil {
		if p.gcsObjectIsPastTTL(gcsMetadata) {
			return status.NotFoundError("backing object may have expired")
		}
	}

	p.sendAtimeUpdate(key, md.GetLastAccessUsec())
	return nil
}

func (p *PebbleCache) gcsObjectIsPastTTL(gcsMetadata *sgpb.StorageMetadata_GCSMetadata) bool {
	// The GCS TTL is set as an integer number of days. The docs are vague,
	// but it seems plausible that if a file is *ever* marked for deletion,
	// it will be deleted, even if it has changed since. Basically, there is
	// one job marking files for deletion, and another job deleting them,
	// and if the object has changed between those two events, it is still
	// deleted.
	//
	// For this reason, if a GCS object is ever less than 1 hour away from
	// TTL, assume it has already been marked for deletion.
	customTimeUsec := gcsMetadata.GetLastCustomTimeUsec()
	buffer := time.Hour
	return p.clock.Since(time.UnixMicro(customTimeUsec))+buffer > time.Duration(p.gcsTTLDays*24)*time.Hour
}

func (p *PebbleCache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	rc, err := p.Reader(ctx, r, 0, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	buf := bytes.NewBuffer(make([]byte, 0, bufferSize(r)))
	_, err = io.Copy(buf, rc)
	return buf.Bytes(), err
}

func bufferSize(r *rspb.ResourceName) int {
	if r.GetCacheType() != rspb.CacheType_CAS {
		// The median and average AC results are less than 4KiB: go/action-result-size
		return 4 * 1024
	}
	// Clamp the size between 0 and 10MB, to protect from invalid and
	// malicious requests.
	return min(10_000_000, max(0, int(r.GetDigest().GetSizeBytes())))
}

func (p *PebbleCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	foundMap := make(map[*repb.Digest][]byte, len(resources))

	for _, r := range resources {
		rc, err := p.reader(ctx, db, r, 0, 0)
		if err != nil {
			if status.IsNotFoundError(err) || os.IsNotExist(err) {
				continue
			}
			return nil, err
		}

		buf := bytes.NewBuffer(make([]byte, 0, bufferSize(r)))
		_, copyErr := io.Copy(buf, rc)
		closeErr := rc.Close()
		if copyErr != nil {
			log.Warningf("[%s] GetMulti encountered error when copying %s: %s", p.name, r.GetDigest().GetHash(), copyErr)
			continue
		}
		if closeErr != nil {
			log.Warningf("[%s] GetMulti cannot close reader when copying %s: %s", p.name, r.GetDigest().GetHash(), closeErr)
			continue
		}
		foundMap[r.GetDigest()] = buf.Bytes()
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

func (p *PebbleCache) sendSizeUpdate(partID string, cacheType rspb.CacheType, op sizeUpdateOp, md *sgpb.FileMetadata, key []byte) {
	delta := getSizeOnLocalDisk(key, md, p.includeMetadataSize)

	if op == deleteSizeOp {
		delta = -1 * delta
	}
	up := &sizeUpdate{
		partID:         partID,
		lastModifyUsec: md.GetLastModifyUsec(),
		groupID:        md.GetFileRecord().GetIsolation().GetGroupId(),
		cacheType:      cacheType,
		delta:          delta,
	}
	p.edits <- up
}

func (p *PebbleCache) sendAtimeUpdate(key filestore.PebbleKey, lastAccessUsec int64) {
	atime := time.UnixMicro(lastAccessUsec)

	metrics.PebbleCacheAtimeDeltaWhenRead.With(prometheus.Labels{
		metrics.CacheNameLabel: p.name,
	}).Observe(float64(time.Since(atime).Milliseconds()))

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
			log.Warningf("[%s] Dropping atime update for %q", p.name, key.String())
		}
	}
}

// The key should be locked before calling this function.
func (p *PebbleCache) deleteMetadataOnly(ctx context.Context, key filestore.PebbleKey) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	// First, lookup the FileMetadata. If it's not found, we don't have the file.
	fileMetadata := sgpb.FileMetadataFromVTPool()
	defer fileMetadata.ReturnToVTPool()
	version, err := p.lookupFileMetadataAndVersion(ctx, db, key, fileMetadata)
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
	p.sendSizeUpdate(fileMetadata.GetFileRecord().GetIsolation().GetPartitionId(), key.CacheType(), deleteSizeOp, fileMetadata, fileMetadataKey)
	return nil
}

func (p *PebbleCache) deleteFileAndMetadata(ctx context.Context, key filestore.PebbleKey, version filestore.PebbleKeyVersion, md *sgpb.FileMetadata) error {
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
	case storageMetadata.GetGcsMetadata() != nil:
		if err := p.fileStorer.DeleteStoredBlob(ctx, storageMetadata.GetGcsMetadata()); err != nil {
			return err
		}
	case storageMetadata.GetChunkedMetadata() != nil:
		break
	default:
		return status.FailedPreconditionErrorf("Unnown storage metadata type: %+v", storageMetadata)
	}

	p.sendSizeUpdate(partitionID, key.CacheType(), deleteSizeOp, md, keyBytes)
	return nil
}

func getSizeOnLocalDisk(key []byte, md *sgpb.FileMetadata, includeMetadata bool) int64 {
	mdSize := int64(md.SizeVT()) + int64(len(key))
	payloadSize := int64(0)

	storageMetadata := md.GetStorageMetadata()
	switch {
	case storageMetadata.GetFileMetadata() != nil:
		payloadSize = md.GetStoredSizeBytes()
	case storageMetadata.GetInlineMetadata() != nil:
		payloadSize = md.GetStoredSizeBytes()
		mdSize -= payloadSize
	case storageMetadata.GetGcsMetadata() != nil:
		payloadSize = 0
	case storageMetadata.GetChunkedMetadata() != nil:
		payloadSize = 0
	}
	if includeMetadata {
		return payloadSize + mdSize
	}
	return payloadSize
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

	md := sgpb.FileMetadataFromVTPool()
	defer md.ReturnToVTPool()
	err = p.lookupFileMetadata(ctx, db, key, md)
	if err != nil {
		return err
	}

	// TODO(tylerw): Make version aware.
	if err := p.deleteFileAndMetadata(ctx, key, filestore.UndefinedKeyVersion, md); err != nil {
		log.Errorf("[%s] Error deleting old record %q: %s", p.name, key.String(), err)
		return err
	}
	return nil
}

func (p *PebbleCache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if spn.IsRecording() {
		spn.SetAttributes(
			attribute.String("digest_hash", r.GetDigest().GetHash()),
			attribute.Int64("digest_size", r.GetDigest().GetSizeBytes()))
	}
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
	fileRecord *sgpb.FileRecord
	key        filestore.PebbleKey

	shouldCompress bool
	isCompressed   bool

	chunker         *chunker.Chunker
	isChunkerClosed bool

	mu            sync.Mutex // protects writtenChunks, numChunks, firstChunk, fileType
	numChunks     int
	firstChunk    []byte
	fileType      sgpb.FileMetadata_FileType
	writtenChunks []*rspb.ResourceName

	eg *errgroup.Group
}

func (p *PebbleCache) newCDCCommitedWriteCloser(ctx context.Context, fileRecord *sgpb.FileRecord, key filestore.PebbleKey, shouldCompress bool, isCompressed bool) (interfaces.CommittedWriteCloser, error) {
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
			cdcw.fileType = sgpb.FileMetadata_COMPLETE_FILE_TYPE
			// When there is only one single chunk, we want to store the original
			// file record with the original key instead of computed digest from
			// the chunkData. This is because the chunkData can be compressed or
			// encrypted, so the digest computed from it will be different from
			// the original digest.
			return cdcw.writeRawChunk(cdcw.fileRecord, cdcw.key, cdcw.firstChunk)
		}
		now := p.clock.Now().UnixMicro()

		md := &sgpb.FileMetadata{
			FileRecord:      fileRecord,
			StorageMetadata: cdcw.Metadata(),
			// The chunks the file record pointed are stored seperately and are
			// not evicted when this entry is evicted. Therefore, the stored
			// size bytes should be zero to avoid double counting.
			StoredSizeBytes: 0,
			LastAccessUsec:  now,
			LastModifyUsec:  now,
			FileType:        sgpb.FileMetadata_COMPLETE_FILE_TYPE,
		}

		if numChunks := len(md.GetStorageMetadata().GetChunkedMetadata().GetResource()); numChunks <= 1 {
			log.Errorf("[%s] expected to have more than one chunks, but actually have %d for digest %s", p.name, numChunks, fileRecord.GetDigest().GetHash())
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
		cdcw.fileType = sgpb.FileMetadata_CHUNK_FILE_TYPE
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

func (cdcw *cdcWriter) writeRawChunk(fileRecord *sgpb.FileRecord, key filestore.PebbleKey, chunkData []byte) error {
	ctx := cdcw.ctx
	p := cdcw.pc

	cwc, err := p.newWrappedWriter(ctx, fileRecord, key, cdcw.shouldCompress || cdcw.isCompressed, cdcw.fileType)
	if err != nil {
		return err
	}
	defer cwc.Close()
	_, err = cwc.Write(chunkData)
	if err != nil {
		return status.InternalErrorf("failed to write raw chunk: %s", err)
	}
	if err := cwc.Commit(); err != nil {
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

func (cdcw *cdcWriter) Metadata() *sgpb.StorageMetadata {
	return &sgpb.StorageMetadata{
		ChunkedMetadata: &sgpb.StorageMetadata_ChunkedMetadata{
			Resource: cdcw.writtenChunks,
		},
	}
}

// zstdCompressor compresses bytes before writing them to the nested writer
type zstdCompressor struct {
	cacheName string

	interfaces.CommittedWriteCloser
	compressBuf     []byte
	poolCompressBuf []byte // Buffer we must return to the pool on close.
	bufferPool      *bytebufferpool.VariableSizePool

	numDecompressedBytes int
	numCompressedBytes   int
}

func NewZstdCompressor(cacheName string, wc interfaces.CommittedWriteCloser, bp *bytebufferpool.VariableSizePool, digestSize int64) *zstdCompressor {
	compressBuf := bp.Get(digestSize)
	return &zstdCompressor{
		cacheName:            cacheName,
		CommittedWriteCloser: wc,
		compressBuf:          compressBuf,
		poolCompressBuf:      compressBuf,
		bufferPool:           bp,
	}
}

func (z *zstdCompressor) Write(decompressedBytes []byte) (int, error) {
	// CompressZstd can allocate a new buffer if the given compressBuf is not large enough
	// to fit the compressed bytes.
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

	z.bufferPool.Put(z.poolCompressBuf)
	return z.CommittedWriteCloser.Close()
}

func (p *PebbleCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	ctx, spn := tracing.StartSpan(ctx)
	if spn.IsRecording() {
		spn.SetAttributes(
			attribute.String("digest_hash", r.GetDigest().GetHash()),
			attribute.Int64("digest_size", r.GetDigest().GetSizeBytes()))
	}
	defer spn.End()
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

	if p.averageChunkSizeBytes > 0 && r.GetDigest().GetSizeBytes() >= int64(p.averageChunkSizeBytes) {
		// Files smaller than averageChunkSizeBytes are highly like to only
		// have one chunk, so we skip cdc-chunking step.
		return p.newCDCCommitedWriteCloser(ctx, fileRecord, key, shouldCompress, isCompressed)
	}

	return p.newWrappedWriter(ctx, fileRecord, key, shouldCompress, sgpb.FileMetadata_COMPLETE_FILE_TYPE)
}

// newWrappedWriter returns an interfaces.CommittedWriteCloser that on Write
// will:
// (1) compress the data if shouldCompress is true; and then
// (2) encrypt the data if encryption is enabled
// (3) write the data using input wcm's Write method.
// On Commit, it will write the metadata for fileRecord.
func (p *PebbleCache) newWrappedWriter(ctx context.Context, fileRecord *sgpb.FileRecord, key filestore.PebbleKey, shouldCompress bool, fileType sgpb.FileMetadata_FileType) (interfaces.CommittedWriteCloser, error) {
	var wcm interfaces.MetadataWriteCloser
	if fileRecord.GetDigest().GetSizeBytes() < p.maxInlineFileSizeBytes {
		wcm = p.fileStorer.InlineWriter(ctx, fileRecord.GetDigest().GetSizeBytes())
	} else if fileRecord.GetDigest().GetSizeBytes() >= p.minGCSFileSizeBytes {
		bw, err := p.fileStorer.BlobWriter(ctx, fileRecord)
		if err != nil {
			return nil, err
		}
		wcm = bw
	} else {
		blobDir := p.blobDir()
		fw, err := p.fileStorer.FileWriter(ctx, blobDir, fileRecord)
		if err != nil {
			return nil, err
		}
		wcm = fw
	}
	// Grab another lease and pass the Close function to the writer
	// so it will be closed when the writer is.
	db, err := p.leaser.DB()
	if err != nil {
		return nil, err
	}

	var encryptionMetadata *sgpb.EncryptionMetadata
	cwc := ioutil.NewCustomCommitWriteCloser(wcm)
	cwc.CloseFn = db.Close
	cwc.CommitFn = func(bytesWritten int64) error {
		now := p.clock.Now().UnixMicro()
		md := &sgpb.FileMetadata{
			FileRecord:         fileRecord,
			StorageMetadata:    wcm.Metadata(),
			EncryptionMetadata: encryptionMetadata,
			StoredSizeBytes:    bytesWritten,
			LastAccessUsec:     now,
			LastModifyUsec:     now,
			FileType:           fileType,
		}
		if bytesWritten == 0 {
			log.Infof("Rejecting zero-length write. Key %q, md: %+v", key, md)
			return status.UnavailableError("zero-length writes are not allowed")
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

func (p *PebbleCache) writeMetadata(ctx context.Context, db pebble.IPebbleDB, key filestore.PebbleKey, md *sgpb.FileMetadata) error {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()

	protoBytes, err := proto.Marshal(md)
	if err != nil {
		return err
	}

	if md.GetFileRecord().GetCompressor() == repb.Compressor_ZSTD {
		labels := prometheus.Labels{metrics.CacheNameLabel: p.name, metrics.CompressionType: "zstd"}
		metrics.CompressedBlobSizeWrite.With(labels).Add(float64(md.GetStoredSizeBytes()))
		metrics.DecompressedBlobSizeWrite.With(labels).Add(float64(md.GetFileRecord().GetDigest().GetSizeBytes()))
	}

	unlockFn := p.locker.Lock(key.LockID())
	defer unlockFn()

	oldMD := sgpb.FileMetadataFromVTPool()
	defer oldMD.ReturnToVTPool()
	if version, err := p.lookupFileMetadataAndVersion(ctx, db, key, oldMD); err == nil {
		oldKeyBytes, err := key.Bytes(version)
		if err != nil {
			return err
		}
		if err := db.Delete(oldKeyBytes, pebble.NoSync); err != nil {
			return err
		}
		p.sendSizeUpdate(oldMD.GetFileRecord().GetIsolation().GetPartitionId(), key.CacheType(), deleteSizeOp, oldMD, oldKeyBytes)
	}

	keyBytes, err := key.Bytes(p.activeDatabaseVersion())
	if err != nil {
		return err
	}

	if err = db.Set(keyBytes, protoBytes, pebble.NoSync); err == nil {
		if key.EncryptionKeyID() != md.GetEncryptionMetadata().GetEncryptionKeyId() && len(md.GetStorageMetadata().GetChunkedMetadata().GetResource()) == 0 {
			err := status.FailedPreconditionErrorf("key vs metadata encryption mismatch for %q: %q vs %q", string(keyBytes), key.EncryptionKeyID(), md.GetEncryptionMetadata().GetEncryptionKeyId())
			alert.UnexpectedEvent("key_metadata_encryption_mismatch", err.Error())
			return err
		}

		partitionID := md.GetFileRecord().GetIsolation().GetPartitionId()
		p.sendSizeUpdate(partitionID, key.CacheType(), addSizeOp, md, keyBytes)

		chunkedMD := md.GetStorageMetadata().GetChunkedMetadata()

		sizeBytes := md.GetStoredSizeBytes()
		for _, cm := range chunkedMD.GetResource() {
			// For an entry that points to multiple chunks, the file size is the
			// sum of the size of the chunks instead of stored_size_bytes.
			sizeBytes += cm.GetDigest().GetSizeBytes()
		}
		if md.GetFileType() == sgpb.FileMetadata_COMPLETE_FILE_TYPE {
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
func (p *PebbleCache) TestingWaitForGC(ctx context.Context) error {
	for {
		p.statusMu.Lock()
		evictors := p.evictors
		p.statusMu.Unlock()

		done := 0
		for _, e := range evictors {
			e.mu.Lock()
			e.lru.UpdateSizeBytes(e.sizeBytes)
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
		select {
		case <-ctx.Done():
			return status.CanceledError("context canceled waiting for GC")
		case <-time.After(100 * time.Millisecond): // Take a break so we don't busy loop
		}
	}
	return nil
}

type evictionKey struct {
	bytes           []byte
	storageMetadata *sgpb.StorageMetadata
}

func (k *evictionKey) ID() string {
	return string(k.bytes)
}

func (k *evictionKey) String() string {
	return string(k.bytes)
}

type partitionEvictor struct {
	ctx           context.Context
	mu            *sync.Mutex
	part          disk.Partition
	fileStorer    filestore.Store
	cacheName     string
	blobDir       string
	dbGetter      pebble.Leaser
	locker        lockmap.Locker
	versionGetter versionGetter
	samples       chan *approxlru.Sample[*evictionKey]
	deletes       chan *approxlru.Sample[*evictionKey]
	rng           *rand.Rand
	clock         clockwork.Clock

	lru         *approxlru.LRU[*evictionKey]
	sizeBytes   int64
	sizeByGroup map[string]int64
	casCount    int64
	acCount     int64

	atimeBufferSize  int
	minEvictionAge   time.Duration
	activeKeyVersion int64

	samplesPerBatch          int
	samplerIterRefreshPeriod time.Duration

	numDeleteWorkers int

	includeMetadataSize bool
}

type versionGetter interface {
	minDatabaseVersion() filestore.PebbleKeyVersion
}

func newPartitionEvictor(ctx context.Context, part disk.Partition, fileStorer filestore.Store, blobDir string, dbg pebble.Leaser, locker lockmap.Locker, vg versionGetter, clock clockwork.Clock, minEvictionAge time.Duration, cacheName string, includeMetadataSize bool, sampleBufferSize int, samplesPerBatch int, samplerIterRefreshPeriod time.Duration, deleteBufferSize int, numDeleteWorkers int) (*partitionEvictor, error) {
	pe := &partitionEvictor{
		ctx:                      ctx,
		mu:                       &sync.Mutex{},
		part:                     part,
		fileStorer:               fileStorer,
		blobDir:                  blobDir,
		dbGetter:                 dbg,
		locker:                   locker,
		versionGetter:            vg,
		rng:                      rand.New(rand.NewSource(time.Now().UnixNano())),
		clock:                    clock,
		minEvictionAge:           minEvictionAge,
		cacheName:                cacheName,
		samples:                  make(chan *approxlru.Sample[*evictionKey], sampleBufferSize),
		samplesPerBatch:          samplesPerBatch,
		samplerIterRefreshPeriod: samplerIterRefreshPeriod,
		deletes:                  make(chan *approxlru.Sample[*evictionKey], deleteBufferSize),
		numDeleteWorkers:         numDeleteWorkers,
		includeMetadataSize:      includeMetadataSize,
		sizeByGroup:              make(map[string]int64),
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
		Clock:                       clock,
		OnEvict:                     pe.evict,
		OnSample:                    pe.sample,
	})
	if err != nil {
		return nil, err
	}
	pe.lru = l

	start := time.Now()
	log.Infof("Pebble Cache [%s]: Initializing cache partition %q...", pe.cacheName, part.ID)
	sizeBytes, casCount, acCount, err := pe.computeSize()
	if err != nil {
		return nil, err
	}
	pe.sizeBytes = sizeBytes
	pe.casCount = casCount
	pe.acCount = acCount
	pe.lru.UpdateSizeBytes(sizeBytes)

	log.Infof("Pebble Cache [%s]: Initialized cache partition %q AC: %d, CAS: %d, Size: %d [bytes] in %s", pe.cacheName, part.ID, pe.acCount, pe.casCount, pe.sizeBytes, time.Since(start))
	return pe, nil
}

func (e *partitionEvictor) startSampleGenerator(quitChan chan struct{}) {
	e.generateSamplesForEviction(quitChan)
	// Drain samples chan before exiting
	for len(e.samples) > 0 {
		<-e.samples
	}
	close(e.samples)
}

func (e *partitionEvictor) processEviction(quitChan chan struct{}) {
	eg := &errgroup.Group{}
	for i := 0; i < e.numDeleteWorkers; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-quitChan:
					return nil
				case sampleToDelete := <-e.deletes:
					e.doEvict(sampleToDelete)
				}
			}
		})
	}
	eg.Wait()
	for len(e.deletes) > 0 {
		<-e.deletes
	}
}

func (e *partitionEvictor) generateSamplesForEviction(quitChan chan struct{}) error {
	db, err := e.dbGetter.DB()
	if err != nil {
		log.Warningf("[%s] cannot generate samples for eviction: failed to get db: %s", e.cacheName, err)
		return err
	}
	defer db.Close()

	randomKeyBuf := make([]byte, 64)
	start, end := keyRange([]byte(e.partitionKeyPrefix() + "/"))
	leftInBatch := 0
	var iterCreatedAt time.Time
	var iter pebble.Iterator
	// We update the iter variable later on, so we need to wrap the Close call
	// in a func to operate on the correct iterator instance.
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	fileMetadata := sgpb.FileMetadataFromVTPool()
	defer fileMetadata.ReturnToVTPool()
	timer := e.clock.NewTimer(0)
	defer timer.Stop()

	// Files are kept in random order (because they are keyed by digest), so
	// instead of doing a new seek for every random sample we will seek once
	// and just read forward, yielding digests until we've found enough.
	for {
		select {
		case <-quitChan:
			return nil
		default:
		}

		// When we started to populate a cache, we cannot find any eligible
		// entries to evict. We will sleep for some time to prevent from
		// constantly generating samples in vain.
		e.mu.Lock()
		shouldSleep := e.sizeBytes <= int64(SamplerSleepThreshold*float64(e.part.MaxSizeBytes))
		e.mu.Unlock()
		if shouldSleep {
			select {
			case <-quitChan:
				return nil
			case <-e.clock.After(SamplerSleepDuration):
			}
		}

		// Refresh the iterator once a while
		if leftInBatch <= 0 || time.Since(iterCreatedAt) > e.samplerIterRefreshPeriod {
			leftInBatch = e.samplesPerBatch
			iterCreatedAt = time.Now()
			// This iterator won't be positioned (Valid() will return false),
			// so we will position it below.
			newIter, err := db.NewIter(&pebble.IterOptions{
				LowerBound: start,
				UpperBound: end,
			})
			if err != nil {
				return err
			}
			if iter != nil {
				iter.Close()
			}
			iter = newIter
			// Decay away sampled partition sizes over time.  This value is kinda arbitrary,
			// but was chosen based on the default sampler refresh period of 5 minutes to
			// ensure that samples mostly decay within a couple hours (normally it's faster
			// than this, assuming eviction is firing on all cylinders).
			e.mu.Lock()
			for k, v := range e.sizeByGroup {
				e.sizeByGroup[k] = int64(float64(v) * (*groupSizeSampleDecayRate))
			}
			e.mu.Unlock()
		}
		leftInBatch--
		if !iter.Valid() {
			// This happens when we create a new iterator or exhaust the
			// existing one.
			randomKey, err := e.randomKey(randomKeyBuf)
			if err != nil {
				log.Warningf("[%s] cannot generate samples for eviction: failed to get random key: %s", e.cacheName, err)
				return err
			}
			if valid := iter.SeekGE(randomKey); !valid {
				leftInBatch = 0 // Force creating a new iterator
				continue
			}
		}

		fileMetadata.ResetVT() // UnmarshalVT doesn't reset, unlike proto.Unmarshal.
		err = fileMetadata.UnmarshalVT(iter.Value())
		if err != nil {
			log.Warningf("[%s] cannot generate sample for eviction, skipping: failed to read proto: %s", e.cacheName, err)
			continue
		}

		if rand.Float64() <= *groupSizeSampleRate {
			keyBytes := make([]byte, len(iter.Key()))
			copy(keyBytes, iter.Key())
			sizeBytes := getSizeOnLocalDisk(keyBytes, fileMetadata, e.includeMetadataSize)
			e.mu.Lock()
			e.sizeByGroup[fileMetadata.GetFileRecord().GetIsolation().GetGroupId()] += sizeBytes
			e.mu.Unlock()
		}

		e.maybeAddToSampleChan(iter, fileMetadata, quitChan, timer)
		iter.Next()
	}
}

func (e *partitionEvictor) maybeAddToSampleChan(iter pebble.Iterator, fileMetadata *sgpb.FileMetadata, quitChan chan struct{}, timer clockwork.Timer) {
	atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
	age := e.clock.Since(atime)
	if age < e.minEvictionAge {
		return
	}
	keyBytes := make([]byte, len(iter.Key()))
	copy(keyBytes, iter.Key())

	sizeBytes := getSizeOnLocalDisk(keyBytes, fileMetadata, e.includeMetadataSize)
	sample := &approxlru.Sample[*evictionKey]{
		Key: &evictionKey{
			bytes:           keyBytes,
			storageMetadata: fileMetadata.GetStorageMetadata(),
		},
		SizeBytes: sizeBytes,
		Timestamp: atime,
	}
	timer.Reset(SamplerSleepDuration)
	select {
	case e.samples <- sample:
	case <-quitChan:
	case <-timer.Chan(): // e.samples is full.
	}
}

func (e *partitionEvictor) updateMetrics() {
	e.mu.Lock()
	defer e.mu.Unlock()
	lbls := prometheus.Labels{metrics.PartitionID: e.part.ID, metrics.CacheNameLabel: e.cacheName}
	metrics.DiskCachePartitionSizeBytes.With(lbls).Set(float64(e.sizeBytes))
	metrics.DiskCachePartitionCapacityBytes.With(lbls).Set(float64(e.part.MaxSizeBytes))
	metrics.PebbleCacheEvictionSamplesChanSize.With(lbls).Set(float64(len(e.samples)))

	metrics.DiskCachePartitionNumItems.With(prometheus.Labels{
		metrics.PartitionID:    e.part.ID,
		metrics.CacheNameLabel: e.cacheName,
		metrics.CacheTypeLabel: "ac"}).Set(float64(e.acCount))
	metrics.DiskCachePartitionNumItems.With(prometheus.Labels{
		metrics.PartitionID:    e.part.ID,
		metrics.CacheNameLabel: e.cacheName,
		metrics.CacheTypeLabel: "cas"}).Set(float64(e.casCount))

	estimatedSizeByGroup := computeEstimatedSizeByGroup(e.sizeByGroup, e.sizeBytes)
	for g, sizeBytes := range estimatedSizeByGroup {
		metrics.DiskCacheSampledPartitionGroupSizeBytes.With(prometheus.Labels{
			metrics.PartitionID:    e.part.ID,
			metrics.CacheNameLabel: e.cacheName,
			metrics.GroupID:        g}).Set(float64(sizeBytes))
	}
}

func (e *partitionEvictor) updateSize(cacheType rspb.CacheType, lastModifyUsec int64, groupID string, deltaSize int64) {
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
		log.Infof("[%s] Cannot update cache size: resource of unknown type", e.cacheName)
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
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return 0, 0, 0, err
	}
	defer iter.Close()
	iter.SeekLT(start)

	casCount := int64(0)
	acCount := int64(0)
	totalSizeBytes := int64(0)
	fileMetadata := sgpb.FileMetadataFromVTPool()
	defer fileMetadata.ReturnToVTPool()

	for iter.Next() {
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			return 0, 0, 0, err
		}

		sizeBytes := getSizeOnLocalDisk(iter.Key(), fileMetadata, true)
		totalSizeBytes += sizeBytes

		// identify and count CAS vs AC files.
		if bytes.Contains(iter.Key(), casDir) {
			casCount += 1
		} else if bytes.Contains(iter.Key(), acDir) {
			acCount += 1
		} else {
			log.Warningf("[%s] Unidentified file (not CAS or AC): %q", e.cacheName, iter.Key())
		}
		fileMetadata.ResetVT()
	}

	return totalSizeBytes, casCount, acCount, nil
}

func partitionMetadataKey(partID string) []byte {
	var key []byte
	key = append(key, SystemKeyPrefix...)
	key = append(key, []byte(partID)...)
	key = append(key, []byte("/metadata")...)
	return key
}

func (e *partitionEvictor) lookupPartitionMetadata() (*sgpb.PartitionMetadata, error) {
	db, err := e.dbGetter.DB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	partitionMD := &sgpb.PartitionMetadata{}
	err = pebble.GetProto(db, partitionMetadataKey(e.part.ID), partitionMD)
	if err != nil {
		return nil, err
	}

	return partitionMD, nil
}

func (e *partitionEvictor) writePartitionMetadata(db pebble.IPebbleDB, md *sgpb.PartitionMetadata) error {
	bs, err := proto.Marshal(md)
	if err != nil {
		return err
	}
	unlockFn := e.locker.Lock(string(partitionMetadataKey(e.part.ID)))
	defer unlockFn()
	return db.Set(partitionMetadataKey(e.part.ID), bs, pebble.Sync)
}

func (e *partitionEvictor) flushPartitionMetadata(db pebble.IPebbleDB) error {
	sizeBytes, _, casCount, acCount := e.Counts()
	return e.writePartitionMetadata(db, &sgpb.PartitionMetadata{
		SizeBytes: sizeBytes,
		CasCount:  casCount,
		AcCount:   acCount,
	})
}

func (e *partitionEvictor) computeSize() (int64, int64, int64, error) {
	if !*forceCalculateMetadata {
		partitionMD, err := e.lookupPartitionMetadata()
		if err == nil {
			log.Infof("[%s] Loaded partition %q metadata from cache: %+v", e.cacheName, e.part.ID, partitionMD)
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

	partitionMD := &sgpb.PartitionMetadata{
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

func (e *partitionEvictor) Counts() (int64, map[string]int64, int64, int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sizeBytes, maps.Clone(e.sizeByGroup), e.casCount, e.acCount
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

func (e *partitionEvictor) randomKey(buf []byte) ([]byte, error) {
	// If the database is migrating keys, then sampling at
	// minDatabaseVersion will evict the oldest data first. If
	// the database is up to date, then minDatabaseVersion will be the same
	// as maxDatabaseVersion, and this will sample all data.
	version := e.versionGetter.minDatabaseVersion()
	digestLength := len(buf)
	for i := 0; i < digestLength; i++ {
		buf[i] = digestChars[e.rng.Intn(len(digestChars))]
	}

	key, err := e.fileStorer.PebbleKey(&sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:   rspb.CacheType_CAS,
			PartitionId: e.part.ID,
			// Empty GroupID
		},
		Digest: &repb.Digest{
			Hash: string(buf),
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

func (e *partitionEvictor) evict(ctx context.Context, sample *approxlru.Sample[*evictionKey]) error {
	e.deletes <- sample
	return nil
}

func (e *partitionEvictor) doEvict(sample *approxlru.Sample[*evictionKey]) {
	db, err := e.dbGetter.DB()
	if err != nil {
		log.Warningf("[%s] unable to get db: %s", e.cacheName, err)
		return
	}
	defer db.Close()

	var key filestore.PebbleKey
	_, err = key.FromBytes(sample.Key.bytes)
	if err != nil {
		log.Warningf("[%s] unable to read key %s: %s", e.cacheName, sample.Key, err)
		return
	}
	unlockFn := e.locker.Lock(key.LockID())
	defer unlockFn()

	md := sgpb.FileMetadataFromVTPool()
	err = pebble.GetProto(db, sample.Key.bytes, md)
	defer md.ReturnToVTPool()
	if err != nil {
		log.Infof("[%s] failed to read file metadata for key %s: %s", e.cacheName, sample.Key, err)
		return
	}
	atime := time.UnixMicro(md.GetLastAccessUsec())
	age := time.Since(atime)
	if !sample.Timestamp.Equal(atime) {
		// atime have been updated. Do not evict.
		return
	}

	if err := e.deleteFile(sample.Key.bytes, key, md.GetFileRecord().GetIsolation().GetGroupId(), md.GetLastModifyUsec(), sample.SizeBytes, sample.Key.storageMetadata); err != nil {
		log.Errorf("[%s] Error evicting file for key %q: %s (ignoring)", e.cacheName, sample.Key, err)
		return
	}
	lbls := prometheus.Labels{metrics.PartitionID: e.part.ID, metrics.CacheNameLabel: e.cacheName}
	metrics.DiskCacheNumEvictions.With(lbls).Inc()
	metrics.DiskCacheBytesEvicted.With(lbls).Add(float64(sample.SizeBytes))
	metrics.DiskCacheEvictionAgeMsec.With(lbls).Observe(float64(age.Milliseconds()))
	metrics.DiskCacheLastEvictionAgeUsec.With(lbls).Set(float64(age.Microseconds()))
}

func (e *partitionEvictor) sample(ctx context.Context, k int) ([]*approxlru.Sample[*evictionKey], error) {
	samples := make([]*approxlru.Sample[*evictionKey], 0, k)
	for i := 0; i < k; i++ {
		s, ok := <-e.samples
		if ok {
			samples = append(samples, s)
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

func (e *partitionEvictor) deleteFile(rawKey []byte, key filestore.PebbleKey, groupID string, lastModifyUsec int64, storedSizeBytes int64, storageMetadata *sgpb.StorageMetadata) error {
	db, err := e.dbGetter.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Delete(rawKey, pebble.NoSync); err != nil {
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
	case storageMetadata.GetGcsMetadata() != nil:
		if err := e.fileStorer.DeleteStoredBlob(context.TODO(), storageMetadata.GetGcsMetadata()); err != nil {
			return err
		}
	case storageMetadata.GetChunkedMetadata() != nil:
		break
	default:
		return status.FailedPreconditionErrorf("Unnown storage metadata type: %+v", storageMetadata)
	}

	if storedSizeBytes > 0 {
		e.updateSize(key.CacheType(), lastModifyUsec, groupID, -1*storedSizeBytes)
	}
	return nil
}

func (e *partitionEvictor) partitionKeyPrefix() string {
	return filestore.PartitionDirectoryPrefix + e.part.ID
}

func (e *partitionEvictor) run(quitChan chan struct{}) error {
	e.lru.Start()
	<-quitChan
	e.lru.Stop()
	return nil
}

func (p *PebbleCache) flushPartitionMetadata() {
	for _, e := range p.evictors {
		if err := e.flushPartitionMetadata(p.db); err != nil {
			log.Warningf("[%s] could not flush partition metadata: %s", e.cacheName, err)
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

	p.metricsCollector.UpdateMetrics(m, om, p.name)
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
				log.Warningf("[%s] could not retrieve filesystem stats: %s", p.name, err)
			} else {
				metrics.DiskCacheFilesystemTotalBytes.With(prometheus.Labels{metrics.CacheNameLabel: p.name}).Set(float64(fsu.Total))
				metrics.DiskCacheFilesystemAvailBytes.With(prometheus.Labels{metrics.CacheNameLabel: p.name}).Set(float64(fsu.Avail))
			}

			for _, e := range evictors {
				e.updateMetrics()
			}

			if err := p.updatePebbleMetrics(); err != nil {
				log.Warningf("[%s] could not update pebble metrics: %s", p.name, err)
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
	bufferPool  *bytebufferpool.VariableSizePool
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
func (p *PebbleCache) newChunkedReader(ctx context.Context, chunkedMD *sgpb.StorageMetadata_ChunkedMetadata, shouldDecompress bool) (io.ReadCloser, error) {
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
			rc, err := p.Reader(ctx, rn, 0, 0)
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			defer rc.Close()
			if _, err = io.Copy(pw, rc); err != nil {
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
	// Fields in fileMetadata might be used after the function returns, so we are
	// not use mem pooling here.
	fileMetadata := &sgpb.FileMetadata{}
	err = p.lookupFileMetadata(ctx, db, key, fileMetadata)
	unlockFn()
	if err != nil {
		return nil, err
	}

	// If this object was not chunked, and is somehow stored as a zero-
	// length file, pretend it does not exist.
	md := fileMetadata.GetStorageMetadata()
	if chunkedMD := md.GetChunkedMetadata(); chunkedMD == nil && fileMetadata.GetStoredSizeBytes() == 0 {
		log.Infof("Ignoring zero-length file. Key: %q, md: %+v", key, fileMetadata)
		return nil, status.NotFoundError("object not found (zero-length)")
	}
	// If this is a GCS object, ensure the custom time is relatively recent
	// so that we avoid saying something exists when it's been deleted by
	// a GCS lifecycle rule.
	if gcsMetadata := fileMetadata.GetStorageMetadata().GetGcsMetadata(); gcsMetadata != nil {
		if p.gcsObjectIsPastTTL(gcsMetadata) {
			return nil, status.NotFoundError("backing object may have expired")
		}
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
	p.quitChan = make(chan struct{})
	for _, evictor := range p.evictors {
		evictor := evictor
		p.eg.Go(func() error {
			return evictor.run(p.quitChan)
		})
		p.eg.Go(func() error {
			evictor.startSampleGenerator(p.quitChan)
			return nil
		})
		p.eg.Go(func() error {
			evictor.processEviction(p.quitChan)
			return nil
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
	for range p.numAtimeUpdateWorkers {
		p.eg.Go(func() error {
			return p.processAccessTimeUpdates(p.quitChan)
		})
	}
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
	log.Infof("Pebble Cache [%s]: beginning shutdown", p.name)
	close(p.quitChan)
	if err := p.eg.Wait(); err != nil {
		return err
	}
	log.Infof("Pebble Cache [%s]: waitgroups finished", p.name)

	// Flushed db after all waitgroups finished to reduce the change of lost
	// evictions during app restarts
	p.flushPartitionMetadata()
	if err := p.db.Flush(); err != nil {
		return err
	}
	log.Infof("Pebble Cache [%s]: db flushed", p.name)

	// Wait for all active requests to be finished.
	p.leaser.Close()

	log.Infof("Pebble Cache [%s]: finished serving requests", p.name)

	// Wait for all enqueued size updates to be processed.
	close(p.edits)
	if err := p.egSizeUpdates.Wait(); err != nil {
		return err
	}

	log.Infof("Pebble Cache [%s]: finished processing size updates", p.name)

	// Write out the final partition metadata.
	p.flushPartitionMetadata()

	if err := p.db.Flush(); err != nil {
		return err
	}
	log.Infof("Pebble Cache [%s]: db flushed again", p.name)

	if err := p.db.Close(); err != nil {
		return err
	}

	if err := p.locker.Close(); err != nil {
		return err
	}

	return nil
}
