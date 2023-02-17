package pebble_cache

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
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
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockmap"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/cockroachdb/pebble"
	"github.com/docker/go-units"
	"github.com/elastic/gosigar"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"golang.org/x/time/rate"
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

	backgroundRepairFrequency = flag.Duration("cache.pebble.background_repair_frequency", 1*24*time.Hour, "How frequently to run period background repair tasks.")
	backgroundRepairQPSLimit  = flag.Int("cache.pebble.background_repair_qps_limit", 100, "QPS limit for background repair modifications.")
	deleteACEntriesOlderThan  = flag.Duration("cache.pebble.delete_ac_entries_older_than", 0, "If set, the background repair will delete AC entries older than this time.")
	scanForOrphanedFiles      = flag.Bool("cache.pebble.scan_for_orphaned_files", false, "If true, scan for orphaned files")
	orphanDeleteDryRun        = flag.Bool("cache.pebble.orphan_delete_dry_run", true, "If set, log orphaned files instead of deleting them")
	dirDeletionDelay          = flag.Duration("cache.pebble.dir_deletion_delay", time.Hour, "How old directories must be before being eligible for deletion when empty")
	atimeUpdateThresholdFlag  = flag.Duration("cache.pebble.atime_update_threshold", DefaultAtimeUpdateThreshold, "Don't update atime if it was updated more recently than this")
	atimeBufferSizeFlag       = flag.Int("cache.pebble.atime_buffer_size", DefaultAtimeBufferSize, "Buffer up to this many atime updates in a channel before dropping atime updates")
	minEvictionAgeFlag        = flag.Duration("cache.pebble.min_eviction_age", DefaultMinEvictionAge, "Don't evict anything unless it's been idle for at least this long")
	forceCompaction           = flag.Bool("cache.pebble.force_compaction", false, "If set, compact the DB when it's created")
	forceCalculateMetadata    = flag.Bool("cache.pebble.force_calculate_metadata", false, "If set, partition size and counts will be calculated even if cached information is available.")
	samplesPerEviction        = flag.Int("cache.pebble.samples_per_eviction", 20, "How many records to sample on each eviction")
	samplePoolSize            = flag.Int("cache.pebble.sample_pool_size", 500, "How many deletion candidates to maintain between evictions")
	copyPartition             = flag.String("cache.pebble.copy_partition_data", "", "If set, all data will be copied from the source partition to the destination partition on startup. The cache will not serve data while the copy is in progress. Specified in format source_partition_id:destination_partition_id,")

	activeKeyVersion = flag.Int64("cache.pebble.active_key_version", int64(filestore.UndefinedKeyVersion), "The key version new data will be written with")

	// Compression related flags
	minBytesAutoZstdCompression = flag.Int64("cache.pebble.min_bytes_auto_zstd_compression", 0, "Blobs larger than this will be zstd compressed before written to disk.")
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

	AtimeUpdateThreshold *time.Duration
	AtimeBufferSize      *int
	MinEvictionAge       *time.Duration
}

type sizeUpdate struct {
	partID    string
	cacheType resource.CacheType
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

	atimeUpdateThreshold time.Duration
	atimeBufferSize      int
	minEvictionAge       time.Duration

	lastDBVersion filestore.PebbleKeyVersion

	env    environment.Env
	db     *pebble.DB
	leaser pebbleutil.Leaser
	locker lockmap.Locker

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
}

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
		atimeBufferSize:             *opts.AtimeBufferSize,
		minEvictionAge:              *opts.MinEvictionAge,
		env:                         env,
		db:                          db,
		leaser:                      pebbleutil.NewDBLeaser(db),
		locker:                      lockmap.New(),
		brokenFilesDone:             make(chan struct{}),
		orphanedFilesDone:           make(chan struct{}),
		eg:                          &errgroup.Group{},
		egSizeUpdates:               &errgroup.Group{},
		statusMu:                    &sync.Mutex{},
		edits:                       make(chan *sizeUpdate, 1000),
		accesses:                    make(chan *accessTimeUpdate, *opts.AtimeBufferSize),
		evictors:                    make([]*partitionEvictor, len(opts.Partitions)),
		fileStorer:                  filestore.New(filestore.Opts{}),
		bufferPool:                  bytebufferpool.New(CompressorBufSizeBytes),
		minBytesAutoZstdCompression: opts.MinBytesAutoZstdCompression,
	}

	versionMetadata, err := pc.databaseVersionMetadata()
	if err != nil {
		return nil, err
	}
	pc.lastDBVersion = filestore.PebbleKeyVersion(versionMetadata.GetVersion())

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
			pe, err := newPartitionEvictor(part, pc.fileStorer, blobDir, pc.leaser, pc.locker, pc, pc.accesses, *opts.AtimeBufferSize, *opts.MinEvictionAge, opts.Name)
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

	buf, err := pebbleutil.GetCopy(db, p.databaseVersionKey())
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

// currentDatabaseVersion returns the currently stored filestore.PebbleKeyVersion.
// It is safe to call this function in a loop -- the underlying metadata will
// only be fetched a max of once per second.
func (p *PebbleCache) currentDatabaseVersion() filestore.PebbleKeyVersion {
	unlockFn := p.locker.RLock(string(p.databaseVersionKey()))
	defer unlockFn()
	return p.lastDBVersion
}

func (p *PebbleCache) activeDatabaseVersion() filestore.PebbleKeyVersion {
	return filestore.PebbleKeyVersion(*activeKeyVersion)
}

// updateDatabaseVersion updates the database version to newVersion.
func (p *PebbleCache) updateDatabaseVersion(newVersion filestore.PebbleKeyVersion) error {
	versionKey := p.databaseVersionKey()
	unlockFn := p.locker.Lock(string(versionKey))
	defer unlockFn()

	oldVersionMetadata, err := p.databaseVersionMetadata()
	if err != nil {
		return err
	}

	newVersionMetadata := proto.Clone(oldVersionMetadata).(*rfpb.VersionMetadata)
	newVersionMetadata.Version = int64(newVersion)
	newVersionMetadata.LastModifyUsec = time.Now().UnixMicro()

	buf, err := proto.Marshal(newVersionMetadata)
	if err != nil {
		return err
	}

	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Set(versionKey, buf, &pebble.WriteOptions{Sync: true}); err != nil {
		return err
	}

	p.lastDBVersion = newVersion

	log.Printf("Pebble Cache: db version changed from %+v to %+v", oldVersionMetadata, newVersionMetadata)
	return nil
}

func (p *PebbleCache) updateAtime(key filestore.PebbleKey) error {
	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	// Write Lock: because we read/modify/write below.
	unlockFn := p.locker.Lock(key.LockID())
	defer unlockFn()

	md, version, err := p.lookupFileMetadataAndVersion(p.env.GetServerContext(), iter, key)
	if err != nil {
		return err
	}

	atime := time.UnixMicro(md.GetLastAccessUsec())
	if !olderThanThreshold(atime, p.atimeUpdateThreshold) {
		return nil
	}
	md.LastAccessUsec = time.Now().UnixMicro()
	protoBytes, err := proto.Marshal(md)
	if err != nil {
		return err
	}
	keyBytes, err := key.Bytes(version)
	if err != nil {
		return err
	}
	return db.Set(keyBytes, protoBytes, &pebble.WriteOptions{Sync: false})
}

func (p *PebbleCache) processAccessTimeUpdates(quitChan chan struct{}) error {
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
			if err := p.updateAtime(accessTimeUpdate.key); err != nil {
				log.Warningf("Error updating atime: %s", err)
			}
		default:
			exitMu.Lock()
			done := exiting
			exitMu.Unlock()

			if done {
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
	fixMissingFiles := true
	fixOLDACEntries := *deleteACEntriesOlderThan != 0

	for {
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

		// Nothing else to do?
		if !fixMissingFiles && !fixOLDACEntries {
			return nil
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

func (p *PebbleCache) backgroundRepairIteration(quitChan chan struct{}, opts *repairOpts) error {
	log.Infof("Pebble Cache: backgroundRepairIteration starting")

	evictors := make(map[string]*partitionEvictor, 0)
	p.statusMu.Lock()
	for _, pe := range p.evictors {
		evictors[pe.part.ID] = pe
	}
	p.statusMu.Unlock()

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
			return nil
		default:
		}

		if bytes.HasPrefix(iter.Key(), SystemKeyPrefix) {
			continue
		}

		if time.Since(lastUpdate) > 1*time.Minute {
			log.Infof("Pebble Cache: backgroundRepairIteration in progress, scanned %s keys, fixed %d missing files, deleted %s old AC entries consuming %s", pr.Sprint(totalCount), missingFiles, pr.Sprint(oldACEntries), units.BytesSize(float64(oldACEntriesBytes)))
			lastUpdate = time.Now()
		}

		totalCount++

		// Attempt a read -- if the file is unreadable; update the metadata.
		fileMetadataKey := iter.Key()
		var key filestore.PebbleKey
		_, err := key.FromBytes(iter.Key())
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

		if !removedEntry && opts.deleteACEntriesOlderThan != 0 && bytes.Contains(fileMetadataKey, acDir) {
			atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
			age := time.Since(atime)
			if age > opts.deleteACEntriesOlderThan {
				e, ok := evictors[fileMetadata.GetFileRecord().GetIsolation().GetPartitionId()]
				if ok {
					_ = modLim.Wait(p.env.GetServerContext())
					err := e.deleteFile(key, fileMetadata.GetStoredSizeBytes(), fileMetadata.GetStorageMetadata())
					if err != nil {
						log.Warningf("Could not delete old AC key %q: %s", string(fileMetadataKey), err)
					} else {
						removedEntry = true
						oldACEntries++
						oldACEntriesBytes += fileMetadata.GetStoredSizeBytes()
					}
				} else {
					log.Warningf("Did not find evictor for %q for key %q", fileMetadata.GetFileRecord().GetIsolation().GetPartitionId(), string(fileMetadataKey))
				}
			}
		}

		if !removedEntry && fileMetadata.GetFileRecord().GetCompressor() == repb.Compressor_IDENTITY {
			uncompressedCount++
			uncompressedBytes += fileMetadata.GetStoredSizeBytes()
		}
	}
	log.Infof("Pebble Cache: backgroundRepairIteration scanned %s records (%s uncompressed entries remaining using %s bytes [%s])", pr.Sprint(totalCount), pr.Sprint(uncompressedCount), pr.Sprint(uncompressedBytes), units.BytesSize(float64(uncompressedBytes)))
	if opts.deleteEntriesWithMissingFiles {
		log.Infof("Pebble Cache: backgroundRepairIteration deleted %d keys with missing files", missingFiles)
	}
	if opts.deleteACEntriesOlderThan != 0 {
		log.Infof("Pebble Cache: backgroundRepairIteration deleted %s AC keys older than %s using %s", pr.Sprint(oldACEntries), opts.deleteACEntriesOlderThan, units.BytesSize(float64(oldACEntriesBytes)))
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
	buf += fmt.Sprintf("Stored data version: %d, new writes version: %d\n", p.currentDatabaseVersion(), p.activeDatabaseVersion())
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

func (p *PebbleCache) lookupGroupAndPartitionID(ctx context.Context, remoteInstanceName string) (string, string, error) {
	groupID := p.userGroupID(ctx)
	for _, pm := range p.partitionMappings {
		if pm.GroupID == groupID && strings.HasPrefix(remoteInstanceName, pm.Prefix) {
			return groupID, pm.PartitionID, nil
		}
	}
	return groupID, DefaultPartitionID, nil
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
func (p *PebbleCache) blobDir() string {
	filePath := filepath.Join(p.rootDirectory, "blobs")
	return filePath
}

func (p *PebbleCache) lookupFileMetadataAndVersion(ctx context.Context, iter *pebble.Iterator, key filestore.PebbleKey) (*rfpb.FileMetadata, filestore.PebbleKeyVersion, error) {
	fileMetadata := &rfpb.FileMetadata{}
	var lastErr error
	for version := p.activeDatabaseVersion(); version >= p.currentDatabaseVersion(); version-- {
		keyBytes, err := key.Bytes(version)
		if err != nil {
			return nil, -1, err
		}
		lastErr = pebbleutil.LookupProto(iter, keyBytes, fileMetadata)
		if lastErr == nil {
			return fileMetadata, version, nil
		}
	}
	return nil, -1, lastErr
}

func (p *PebbleCache) lookupFileMetadata(ctx context.Context, iter *pebble.Iterator, key filestore.PebbleKey) (*rfpb.FileMetadata, error) {
	md, _, err := p.lookupFileMetadataAndVersion(ctx, iter, key)
	return md, err
}

// iterHasKey returns a bool indicating if the provided iterator has the
// exact key specified.
func (p *PebbleCache) iterHasKey(iter *pebble.Iterator, key filestore.PebbleKey) (bool, error) {
	for version := p.activeDatabaseVersion(); version >= p.currentDatabaseVersion(); version-- {
		keyBytes, err := key.Bytes(version)
		if err != nil {
			return false, err
		}
		if iter.SeekGE(keyBytes) && bytes.Compare(iter.Key(), keyBytes) == 0 {
			return true, nil
		}
	}
	return false, nil
}

func readFileMetadata(reader pebble.Reader, key filestore.PebbleKey) (*rfpb.FileMetadata, error) {
	// TODO(tylerw): Make version aware.
	fileMetadataKey, err := key.Bytes(filestore.UndefinedKeyVersion)
	if err != nil {
		return nil, err
	}

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

func (p *PebbleCache) handleMetadataMismatch(ctx context.Context, causeErr error, key filestore.PebbleKey, fileMetadata *rfpb.FileMetadata) bool {
	if !status.IsNotFoundError(causeErr) && !os.IsNotExist(causeErr) {
		return false
	}
	if fileMetadata.GetStorageMetadata().GetFileMetadata() != nil {
		log.Warningf("Handling metadata mismatch for %q: %+v", key.String(), fileMetadata)
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
	key, err := p.fileStorer.PebbleKey(fileRecord)
	if err != nil {
		return false, err
	}
	unlockFn := p.locker.RLock(key.LockID())
	defer unlockFn()

	found, err := p.iterHasKey(iter, key)
	if err != nil {
		return false, err
	}
	log.Debugf("Pebble contains %s is %t", key.String(), found)
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
		key, err := p.fileStorer.PebbleKey(fileRecord)
		if err != nil {
			return nil, err
		}

		unlockFn := p.locker.RLock(key.LockID())
		found, err := p.iterHasKey(iter, key)
		if err != nil {
			return nil, err
		}
		if !found {
			missing = append(missing, r.GetDigest())
		}
		unlockFn()
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
		key, err := p.fileStorer.PebbleKey(fileRecord)
		if err != nil {
			return nil, err
		}

		unlockFn := p.locker.RLock(key.LockID())
		fileMetadata, err := p.lookupFileMetadata(ctx, iter, key)
		unlockFn()
		if err != nil {
			continue
		}

		rc, err := p.readerForCompressionType(ctx, r, key, fileMetadata, 0, 0)
		if err != nil {
			if status.IsNotFoundError(err) || os.IsNotExist(err) {
				unlockFn := p.locker.Lock(key.LockID())
				p.handleMetadataMismatch(ctx, err, key, fileMetadata)
				unlockFn()
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

func (p *PebbleCache) sendSizeUpdate(partID string, cacheType resource.CacheType, delta int64) {
	up := &sizeUpdate{
		partID:    partID,
		cacheType: cacheType,
		delta:     delta,
	}
	p.edits <- up
}

func (p *PebbleCache) sendAtimeUpdate(key filestore.PebbleKey, fileMetadata *rfpb.FileMetadata) {
	atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
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
	// TODO(tylerw): Make version aware.
	fileMetadataKey, err := key.Bytes(filestore.UndefinedKeyVersion)
	if err != nil {
		return err
	}

	db, err := p.leaser.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	// First, lookup the FileMetadata. If it's not found, we don't have the file.
	fileMetadata, err := p.lookupFileMetadata(ctx, iter, key)
	if err != nil {
		return err
	}

	if err := db.Delete(fileMetadataKey, &pebble.WriteOptions{Sync: false}); err != nil {
		return err
	}
	p.sendSizeUpdate(fileMetadata.GetFileRecord().GetIsolation().GetPartitionId(), key.CacheType(), -1*fileMetadata.GetStoredSizeBytes())
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
	default:
		return status.FailedPreconditionErrorf("Unnown storage metadata type: %+v", storageMetadata)
	}

	p.sendSizeUpdate(partitionID, key.CacheType(), -1*md.GetStoredSizeBytes())
	return nil
}

func (p *PebbleCache) Delete(ctx context.Context, r *resource.ResourceName) error {
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

	iter := db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	unlockFn := p.locker.Lock(key.LockID())
	defer unlockFn()
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

	key, err := p.fileStorer.PebbleKey(fileRecord)
	if err != nil {
		return nil, err
	}
	log.Debugf("Attempting pebble reader %s", key.String())

	// First, lookup the FileMetadata. If it's not found, we don't have the file.
	unlockFn := p.locker.RLock(key.LockID())
	fileMetadata, err := p.lookupFileMetadata(ctx, iter, key)
	unlockFn()
	if err != nil {
		return nil, err
	}

	rc, err := p.readerForCompressionType(ctx, r, key, fileMetadata, uncompressedOffset, limit)
	if err != nil {
		if status.IsNotFoundError(err) || os.IsNotExist(err) {
			unlockFn := p.locker.Lock(key.LockID())
			p.handleMetadataMismatch(ctx, err, key, fileMetadata)
			unlockFn()
		}
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
	shouldCompress := r.GetCompressor() == repb.Compressor_IDENTITY && r.GetDigest().GetSizeBytes() >= p.minBytesAutoZstdCompression
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
	key, err := p.fileStorer.PebbleKey(fileRecord)
	if err != nil {
		return nil, err
	}
	log.Debugf("Attempting pebble writer %s", key.String())

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

		iter := db.NewIter(nil /*default iterOptions*/)
		defer iter.Close()

		unlockFn := p.locker.Lock(key.LockID())
		defer unlockFn()

		if oldMD, version, err := p.lookupFileMetadataAndVersion(ctx, iter, key); err == nil {
			if err := p.deleteFileAndMetadata(ctx, key, version, oldMD); err != nil {
				log.Errorf("Error deleting old record %q: %s", key.String(), err)
				return err
			}
		}

		keyBytes, err := key.Bytes(p.activeDatabaseVersion())
		if err != nil {
			return err
		}

		if err = db.Set(keyBytes, protoBytes, &pebble.WriteOptions{Sync: false}); err == nil {
			partitionID := fileRecord.GetIsolation().GetPartitionId()
			p.sendSizeUpdate(partitionID, key.CacheType(), md.GetStoredSizeBytes())
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

type evictionKey struct {
	key             filestore.PebbleKey
	storageMetadata *rfpb.StorageMetadata
}

func (k *evictionKey) ID() string {
	return k.key.String()
}

func (k *evictionKey) String() string {
	return k.key.String()
}

type partitionEvictor struct {
	mu            *sync.Mutex
	part          disk.Partition
	fileStorer    filestore.Store
	cacheName     string
	blobDir       string
	dbGetter      pebbleutil.Leaser
	locker        lockmap.Locker
	versionGetter versionGetter
	accesses      chan<- *accessTimeUpdate
	rng           *rand.Rand

	lru       *approxlru.LRU[*evictionKey]
	sizeBytes int64
	casCount  int64
	acCount   int64

	atimeBufferSize int
	minEvictionAge  time.Duration
}

type versionGetter interface {
	currentDatabaseVersion() filestore.PebbleKeyVersion
}

func newPartitionEvictor(part disk.Partition, fileStorer filestore.Store, blobDir string, dbg pebbleutil.Leaser, locker lockmap.Locker, vg versionGetter, accesses chan<- *accessTimeUpdate, atimeBufferSize int, minEvictionAge time.Duration, cacheName string) (*partitionEvictor, error) {
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
		atimeBufferSize: atimeBufferSize,
		minEvictionAge:  minEvictionAge,
		cacheName:       cacheName,
	}
	l, err := approxlru.New(&approxlru.Opts[*evictionKey]{
		SamplePoolSize:     *samplePoolSize,
		SamplesPerEviction: *samplesPerEviction,
		MaxSizeBytes:       int64(JanitorCutoffThreshold * float64(part.MaxSizeBytes)),
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

	log.Infof("Pebble Cache: Initialized cache partition %q AC: %d, CAS: %d, Size: %d [bytes] in %s", part.ID, pe.acCount, pe.casCount, pe.sizeBytes, time.Since(start))
	return pe, nil
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

func (e *partitionEvictor) updateSize(cacheType resource.CacheType, deltaSize int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	deltaCount := int64(1)
	if deltaSize < 0 {
		deltaCount = -1
	}

	switch cacheType {
	case resource.CacheType_CAS:
		e.casCount += deltaCount
	case resource.CacheType_AC:
		e.acCount += deltaCount
	case resource.CacheType_UNKNOWN_CACHE_TYPE:
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

	partitionMDBuf, err := pebbleutil.GetCopy(db, partitionMetadataKey(e.part.ID))
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
	unlockFn := e.locker.Lock(string(partitionMetadataKey(e.part.ID)))
	defer unlockFn()
	return db.Set(partitionMetadataKey(e.part.ID), bs, &pebble.WriteOptions{Sync: true})
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
	buf += fmt.Sprintf("GC Last run: %s\n", e.lru.LastRun().Format("Jan 02, 2006 15:04:05 MST"))
	lastEvictedStr := "nil"
	if le := e.lru.LastEvicted(); le != nil {
		age := time.Since(le.Timestamp)
		lastEvictedStr = fmt.Sprintf("%q age: %s", le.Key.key.String(), age)
	}
	buf += fmt.Sprintf("Last evicted item: %s\n", lastEvictedStr)
	buf += "</pre>"
	return buf
}

var digestChars = []byte("abcdef1234567890")

func (e *partitionEvictor) randomKey(digestLength int) ([]byte, error) {
	version := e.versionGetter.currentDatabaseVersion()
	buf := bytes.NewBuffer(make([]byte, 0, digestLength))
	for i := 0; i < digestLength; i++ {
		buf.WriteByte(digestChars[e.rng.Intn(len(digestChars))])
	}

	key, err := e.fileStorer.PebbleKey(&rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   resource.CacheType_CAS,
			PartitionId: e.part.ID,
		},
		Digest: &repb.Digest{
			Hash: string(buf.Bytes()),
		},
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

	// TODO(tylerw): Make version aware.
	fileMetadataKey, err := sample.Key.key.Bytes(filestore.UndefinedKeyVersion)
	if err != nil {
		return false, err
	}
	unlockFn := e.locker.Lock(sample.Key.key.LockID())
	defer unlockFn()

	_, closer, err := db.Get(fileMetadataKey)
	if err == pebble.ErrNotFound {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	age := time.Since(sample.Timestamp)
	if err := e.deleteFile(sample.Key.key, sample.SizeBytes, sample.Key.storageMetadata); err != nil {
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

	unlockFn := e.locker.RLock(key.key.LockID())
	defer unlockFn()

	md, err := readFileMetadata(db, key.key)
	if err != nil {
		log.Warningf("could not refresh atime for %q: %s", key.String(), err)
		return true, time.Time{}, nil
	}
	atime := time.UnixMicro(md.GetLastAccessUsec())
	age := time.Since(atime)
	if age < e.minEvictionAge {
		return true, time.Time{}, nil
	}
	return false, atime, nil
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

	samples := make([]*approxlru.Sample[*evictionKey], 0, k)

	// generate k random digests and for each:
	//   - seek to the next valid key, and return that file record
	for i := 0; i < k*2; i++ {
		randKey, err := e.randomKey(64)
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

		fileMetadata := &rfpb.FileMetadata{}
		unlockFn := e.locker.RLock(key.LockID())
		err = proto.Unmarshal(iter.Value(), fileMetadata)
		unlockFn()
		if err != nil {
			return nil, err
		}

		atime := time.UnixMicro(fileMetadata.GetLastAccessUsec())
		age := time.Since(atime)
		if age < e.minEvictionAge {
			continue
		}

		sample := &approxlru.Sample[*evictionKey]{
			Key: &evictionKey{
				key:             key,
				storageMetadata: fileMetadata.GetStorageMetadata(),
			},
			SizeBytes: fileMetadata.GetStoredSizeBytes(),
			Timestamp: atime,
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

func (e *partitionEvictor) deleteFile(key filestore.PebbleKey, storedSizeBytes int64, storageMetadata *rfpb.StorageMetadata) error {
	db, err := e.dbGetter.DB()
	if err != nil {
		return err
	}
	defer db.Close()

	// TODO(tylerw): Make version aware.
	fileMetadataKey, err := key.Bytes(filestore.UndefinedKeyVersion)
	if err != nil {
		return err
	}
	if err := db.Delete(fileMetadataKey, pebble.NoSync); err != nil {
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
	default:
		return status.FailedPreconditionErrorf("Unnown storage metadata type: %+v", storageMetadata)
	}

	e.updateSize(key.CacheType(), -1*storedSizeBytes)
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

func (p *PebbleCache) readerForCompressionType(ctx context.Context, resource *resource.ResourceName, key filestore.PebbleKey, fileMetadata *rfpb.FileMetadata, uncompressedOffset int64, uncompressedLimit int64) (io.ReadCloser, error) {
	blobDir := p.blobDir()
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
		return nil, err
	}
	p.sendAtimeUpdate(key, fileMetadata)

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
		return p.backgroundRepair(p.quitChan)
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
