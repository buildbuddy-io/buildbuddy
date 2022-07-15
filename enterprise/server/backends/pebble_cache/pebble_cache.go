package pebble_cache

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
)

var (
	rootDirectory          = flag.String("cache.pebble.root_directory", "", "The root directory to store the database in.")
	blockCacheSizeBytes    = flag.Int64("cache.pebble.block_cache_size_bytes", 1000*megabyte, "How much ram to give the block cache")
	maxInlineFileSizeBytes = flag.Int64("cache.pebble.max_inline_file_size_bytes", 1024, "Files smaller than this may be inlined directly into pebble")
	partitions             = flagtypes.Slice("cache.pebble.partitions", []disk.Partition{}, "")
	partitionMappings      = flagtypes.Slice("cache.pebble.partition_mappings", []disk.PartitionMapping{}, "")

	// TODO(tylerw): remove most of these flags post-migration.
	migrateFromDiskDir        = flag.String("cache.pebble.migrate_from_disk_dir", "", "If set, attempt to migrate this disk dir to a new pebble cache")
	forceAllowMigration       = flag.Bool("cache.pebble.force_allow_migration", false, "If set, allow migrating into an existing pebble cache")
	clearCacheBeforeMigration = flag.Bool("cache.pebble.clear_cache_before_migration", false, "If set, clear any existing cache content before migrating")
	mirrorActiveDiskCache     = flagtypes.Alias[bool]("cache.disk.enable_live_updates", "cache.pebble.mirror_active_disk_cache")
)

const (
	// cutoffThreshold is the point above which a janitor thread will run
	// and delete the oldest items from the cache.
	JanitorCutoffThreshold = .90

	// janitorCheckPeriod is how often the janitor thread will wake up to
	// check the cache size.
	JanitorCheckPeriod = 1 * time.Second
	megabyte           = 1e6

	defaultPartitionID       = "default"
	partitionDirectoryPrefix = "PT"

	// sampleN is the number of random files to sample when adding a new
	// deletion candidate to the sample pool. Increasing this number
	// makes eviction slower but improves sampled-LRU accuracy.
	sampleN = 10

	// samplePoolSize is the number of deletion candidates to maintain in
	// memory at a time. Increasing this number uses more memory but
	// improves sampled-LRU accuracy.
	samplePoolSize = 100
)

// Options is a struct containing the pebble cache configuration options.
// Once a cache is created, the options may not be changed.
type Options struct {
	RootDirectory       string
	Partitions          []disk.Partition
	PartitionMappings   []disk.PartitionMapping
	MaxSizeBytes        int64
	BlockCacheSizeBytes int64
}

type sizeUpdate struct {
	partID string
	key    []byte
	delta  int64
}

// PebbleCache implements the cache interface by storing metadata in a pebble
// database and storing cache entry contents on disk.
type PebbleCache struct {
	opts *Options

	env       environment.Env
	db        *pebble.DB
	isolation *rfpb.Isolation

	statusMu *sync.Mutex // PROTECTS(evictors)
	atimes   *sync.Map
	edits    chan *sizeUpdate

	quitChan chan struct{}
	eg       *errgroup.Group
	evictors []*partitionEvictor
}

// Register creates a new PebbleCache from the configured flags and sets it in
// the provided env.
func Register(env environment.Env) error {
	if *rootDirectory == "" {
		return nil
	}
	if *clearCacheBeforeMigration {
		if err := os.RemoveAll(*rootDirectory); err != nil {
			return err
		}
	}
	if err := disk.EnsureDirectoryExists(*rootDirectory); err != nil {
		return err
	}
	migrateDir := ""
	if *migrateFromDiskDir != "" {
		// Ensure a pebble DB doesn't already exist if we are migrating.
		// But allow anyway if forceAllowMigration was set.
		desc, err := pebble.Peek(*rootDirectory, vfs.Default)
		if err != nil {
			return err
		}
		if desc.Exists && !*forceAllowMigration {
			log.Warningf("Pebble DB at %q already exists, cannot migrate from disk dir: %q", *rootDirectory, *migrateFromDiskDir)
		} else {
			migrateDir = *migrateFromDiskDir
		}
	}
	opts := &Options{
		RootDirectory:       *rootDirectory,
		Partitions:          *partitions,
		PartitionMappings:   *partitionMappings,
		BlockCacheSizeBytes: *blockCacheSizeBytes,
		MaxSizeBytes:        cache_config.MaxSizeBytes(),
	}
	c, err := NewPebbleCache(env, opts)
	if err != nil {
		return status.InternalErrorf("Error configuring pebble cache: %s", err)
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

	if *mirrorActiveDiskCache {
		dci := env.GetCache()
		dc, ok := dci.(*disk_cache.DiskCache)
		if !ok {
			return status.FailedPreconditionError("Cannot mirror disk cache: none was set")
		}
		adds, removes := dc.LiveUpdatesChan()
		c.ProcessLiveUpdates(adds, removes)
		log.Printf("Pebble Cache: mirroring active disk cache")

		// Return early if we're mirroring the disk cache; we don't
		// want to override the disk cache we're mirroring in the env.
		return nil
	}

	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with pebble_cache.")
	}
	env.SetCache(c)
	return nil
}

// validateOpts validates that each partition mapping references a partition
// and that MaxSizeBytes is non-zero.
func validateOpts(opts *Options) error {
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
	if err := validateOpts(opts); err != nil {
		return nil, err
	}
	if err := disk.EnsureDirectoryExists(opts.RootDirectory); err != nil {
		return nil, err
	}
	ensureDefaultPartitionExists(opts)

	pebbleOptions := defaultPebbleOptions()
	if *blockCacheSizeBytes > 0 {
		c := pebble.NewCache(*blockCacheSizeBytes)
		defer c.Unref()
		pebbleOptions.Cache = c
	}

	db, err := pebble.Open(opts.RootDirectory, pebbleOptions)
	if err != nil {
		return nil, err
	}
	pc := &PebbleCache{
		opts:     opts,
		env:      env,
		db:       db,
		quitChan: make(chan struct{}),
		eg:       &errgroup.Group{},
		statusMu: &sync.Mutex{},
		atimes:   &sync.Map{},
		edits:    make(chan *sizeUpdate, 1000),
		evictors: make([]*partitionEvictor, len(opts.Partitions)),
		isolation: &rfpb.Isolation{
			CacheType:   rfpb.Isolation_CAS_CACHE,
			PartitionId: defaultPartitionID,
		},
	}

	peMu := sync.Mutex{}
	eg := errgroup.Group{}
	for i, part := range opts.Partitions {
		i := i
		part := part
		eg.Go(func() error {
			blobDir := pc.partitionBlobDir(part.ID)
			if err := disk.EnsureDirectoryExists(blobDir); err != nil {
				return err
			}
			pe, err := newPartitionEvictor(part, blobDir, pc.db, pc.atimes)
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

	statusz.AddSection("pebble_cache", "On disk LRU cache", pc)
	return pc, nil
}

func keyRange(key []byte) ([]byte, []byte) {
	start := make([]byte, len(key))
	end := make([]byte, len(key))
	copy(start, key)
	copy(end, key)
	return append(start, constants.MinByte), append(end, constants.MaxByte)
}

func partitionLimits(partID string) ([]byte, []byte) {
	key := []byte(partID + "/")
	return keyRange(key)
}

func (p *PebbleCache) processSizeUpdates(quitChan chan struct{}) {
	evictors := make(map[string]*partitionEvictor, 0)
	p.statusMu.Lock()
	for _, pe := range p.evictors {
		evictors[pe.part.ID] = pe
	}
	p.statusMu.Unlock()

	for {
		select {
		case <-quitChan:
			return
		case edit := <-p.edits:
			e := evictors[edit.partID]
			e.updateSize(edit.key, edit.delta)
		}
	}
}

func (p *PebbleCache) scanForBrokenFiles(quitChan chan struct{}) {
	iter := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{constants.MinByte},
		UpperBound: []byte{constants.MaxByte},
	})
	defer iter.Close()

	iter.SeekGE([]byte{constants.MinByte})
	fileMetadata := &rfpb.FileMetadata{}
	blobDir := ""

	for iter.Next() {
		// Check if we're shutting down; exit if so.
		select {
		case <-quitChan:
			break
		default:
		}

		// Attempt a read -- if the file is unreadable; update the metadata.
		fileMetadataKey := iter.Key()
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			log.Errorf("Error unmarshaling metadata when scanning for broken files: %s", err)
			continue
		}
		blobDir = p.partitionBlobDir(fileMetadata.GetFileRecord().GetIsolation().GetPartitionId())
		_, err := filestore.NewReader(p.env.GetServerContext(), blobDir, fileMetadata.GetStorageMetadata(), 0, 0)
		if err != nil {
			p.handleMetadataMismatch(err, fileMetadataKey, fileMetadata)
		}
	}
}

func (p *PebbleCache) MigrateFromDiskDir(diskDir string) error {
	ch := disk_cache.ScanDiskDirectory(diskDir)

	start := time.Now()
	inserted := 0
	err := p.batchProcessCh(ch, func(batch *pebble.Batch, fileMetadata *rfpb.FileMetadata) error {
		protoBytes, err := proto.Marshal(fileMetadata)
		if err != nil {
			return err
		}
		fileMetadataKey, err := constants.FileMetadataKey(fileMetadata.GetFileRecord())
		if err != nil {
			return err
		}
		inserted += 1
		return batch.Set(fileMetadataKey, protoBytes, nil /*ignored write options*/)
	})
	if err != nil {
		return err
	}
	if err := p.db.Flush(); err != nil {
		return err
	}
	log.Printf("Pebble Cache: Migrated %d files from disk dir %q in %s", inserted, diskDir, time.Since(start))
	return nil
}

type batchEditFn func(batch *pebble.Batch, fileMetadata *rfpb.FileMetadata) error

func (p *PebbleCache) batchProcessCh(ch <-chan *rfpb.FileMetadata, editFn batchEditFn) error {
	batch := p.db.NewBatch()
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
			batch = p.db.NewBatch()
		}
	}
	if batch.Count() > 0 {
		if err := batch.Commit(&pebble.WriteOptions{Sync: true}); err != nil {
			log.Warningf("Error comitting batch: %s", err)
		}
	}
	return nil
}

func (p *PebbleCache) ProcessLiveUpdates(adds, removes <-chan *rfpb.FileMetadata) error {
	for i := 0; i < 3; i++ {
		p.eg.Go(func() error {
			return p.batchProcessCh(adds, func(batch *pebble.Batch, fileMetadata *rfpb.FileMetadata) error {
				protoBytes, err := proto.Marshal(fileMetadata)
				if err != nil {
					return err
				}
				fileMetadataKey, err := constants.FileMetadataKey(fileMetadata.GetFileRecord())
				if err != nil {
					return err
				}
				return batch.Set(fileMetadataKey, protoBytes, nil /*ignored write options*/)
			})
		})
		p.eg.Go(func() error {
			return p.batchProcessCh(removes, func(batch *pebble.Batch, fileMetadata *rfpb.FileMetadata) error {
				fileMetadataKey, err := constants.FileMetadataKey(fileMetadata.GetFileRecord())
				if err != nil {
					return err
				}
				return batch.Delete(fileMetadataKey, nil /*ignored write options*/)
			})
		})
	}

	return nil
}

func (p *PebbleCache) Statusz(ctx context.Context) string {
	p.statusMu.Lock()
	evictors := p.evictors
	p.statusMu.Unlock()

	buf := "<pre>"
	buf += p.db.Metrics().String()
	diskEstimateBytes, err := p.db.EstimateDiskUsage([]byte{constants.MinByte}, []byte{constants.MaxByte})
	if err == nil {
		buf += fmt.Sprintf("Estimated disk usage: %d bytes\n", diskEstimateBytes)
	}
	buf += "</pre>"
	for _, e := range evictors {
		buf += e.Statusz(ctx)
	}
	return buf
}

func (p *PebbleCache) lookupPartitionID(ctx context.Context, remoteInstanceName string) (string, error) {
	auth := p.env.GetAuthenticator()
	if auth == nil {
		return defaultPartitionID, nil
	}
	user, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return defaultPartitionID, nil
	}
	for _, pm := range p.opts.PartitionMappings {
		if pm.GroupID == user.GetGroupID() && strings.HasPrefix(remoteInstanceName, pm.Prefix) {
			return pm.PartitionID, nil
		}
	}
	return defaultPartitionID, nil
}

func (p *PebbleCache) WithIsolation(ctx context.Context, cacheType interfaces.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	partID, err := p.lookupPartitionID(ctx, remoteInstanceName)
	if err != nil {
		return nil, err
	}

	newIsolation := &rfpb.Isolation{}
	switch cacheType {
	case interfaces.CASCacheType:
		newIsolation.CacheType = rfpb.Isolation_CAS_CACHE
	case interfaces.ActionCacheType:
		newIsolation.CacheType = rfpb.Isolation_ACTION_CACHE
	default:
		return nil, status.InvalidArgumentErrorf("Unknown cache type %v", cacheType)
	}
	newIsolation.RemoteInstanceName = remoteInstanceName
	newIsolation.PartitionId = partID

	clone := *p
	clone.isolation = newIsolation
	return &clone, nil
}

func (p *PebbleCache) makeFileRecord(ctx context.Context, d *repb.Digest) (*rfpb.FileRecord, error) {
	_, err := digest.Validate(d)
	if err != nil {
		return nil, err
	}

	return &rfpb.FileRecord{
		Isolation: p.isolation,
		Digest:    d,
	}, nil
}

func (p *PebbleCache) partitionBlobDir(partID string) string {
	partDir := partitionDirectoryPrefix + partID
	return filepath.Join(p.opts.RootDirectory, "blobs", partDir)
}

// blobDir returns a directory path under the root directory, specific to the
// configured partition, where blobs can be stored.
func (p *PebbleCache) blobDir() string {
	return p.partitionBlobDir(p.isolation.GetPartitionId())
}

func (p *PebbleCache) updateAtime(fileMetadataKey []byte) {
	nowNanos := time.Now().UnixNano()

	fmkHash := xxhash.Sum64(fileMetadataKey)
	if a, ok := p.atimes.Load(fmkHash); ok {
		lastAccessNanos := a.(int64)
		durSinceLastAccess := time.Duration(nowNanos-lastAccessNanos) * time.Nanosecond
		metrics.DiskCacheUsecSinceLastAccess.Observe(float64(durSinceLastAccess.Microseconds()))
	}
	p.atimes.Store(fmkHash, nowNanos)
}

func (p *PebbleCache) clearAtime(fileMetadataKey []byte) {
	p.atimes.Delete(xxhash.Sum64(fileMetadataKey))
}

// hasFileMetadata returns a bool indicating if the provided iterator has the
// key specified by fileMetadataKey.
func hasFileMetadata(iter *pebble.Iterator, fileMetadataKey []byte) bool {
	if iter.SeekGE(fileMetadataKey) && bytes.Compare(iter.Key(), fileMetadataKey) == 0 {
		return true
	}
	return false
}

func lookupAndSetFileMetadata(iter *pebble.Iterator, fileMetadataKey []byte, fileMetadata *rfpb.FileMetadata) error {
	found := iter.SeekGE(fileMetadataKey)
	if !found || bytes.Compare(fileMetadataKey, iter.Key()) != 0 {
		return status.NotFoundErrorf("record %q not found", fileMetadataKey)
	}
	if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
		return status.InternalErrorf("error reading record %q metadata", fileMetadataKey)
	}
	return nil
}

func lookupFileMetadata(iter *pebble.Iterator, fileMetadataKey []byte) (*rfpb.FileMetadata, error) {
	fileMetadata := &rfpb.FileMetadata{}
	if err := lookupAndSetFileMetadata(iter, fileMetadataKey, fileMetadata); err != nil {
		return nil, err
	}
	return fileMetadata, nil
}

func (p *PebbleCache) handleMetadataMismatch(err error, fileMetadataKey []byte, fileMetadata *rfpb.FileMetadata) {
	if !status.IsNotFoundError(err) && !os.IsNotExist(err) {
		return
	}
	if fileMetadata.GetStorageMetadata().GetFileMetadata() != nil {
		log.Warningf("Metadata record %q was found but file (%+v) not found on disk: %s", fileMetadataKey, fileMetadata, err)
		if err := p.deleteMetadataOnly(fileMetadataKey); err != nil {
			log.Warningf("Error deleting metadata: %s", err)
		}
	}
}

func (p *PebbleCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	iter := p.db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	fileRecord, err := p.makeFileRecord(ctx, d)
	if err != nil {
		return false, err
	}
	fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
	if err != nil {
		return false, err
	}
	found := hasFileMetadata(iter, fileMetadataKey)
	if found {
		p.updateAtime(fileMetadataKey)
	}
	return found, nil
}

func (p *PebbleCache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	iter := p.db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	fileRecord, err := p.makeFileRecord(ctx, d)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}
	md, err := lookupFileMetadata(iter, fileMetadataKey)
	if err != nil {
		return nil, err
	}
	p.updateAtime(fileMetadataKey)
	return &interfaces.CacheMetadata{SizeBytes: md.GetSizeBytes()}, nil
}

func (p *PebbleCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	iter := p.db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	sort.Slice(digests, func(i, j int) bool {
		return digests[i].GetHash() < digests[j].GetHash()
	})

	var missing []*repb.Digest
	for _, d := range digests {
		fileRecord, err := p.makeFileRecord(ctx, d)
		if err != nil {
			return nil, err
		}
		fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
		if err != nil {
			return nil, err
		}
		if !hasFileMetadata(iter, fileMetadataKey) {
			missing = append(missing, d)
		} else {
			p.updateAtime(fileMetadataKey)
		}
	}
	return missing, nil
}

func (p *PebbleCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	rc, err := p.Reader(ctx, d, 0, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (p *PebbleCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	foundMap := make(map[*repb.Digest][]byte, len(digests))

	sort.Slice(digests, func(i, j int) bool {
		return digests[i].GetHash() < digests[j].GetHash()
	})

	iter := p.db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	blobDir := p.blobDir()
	buf := &bytes.Buffer{}
	fileMetadata := &rfpb.FileMetadata{}
	for _, d := range digests {
		fileRecord, err := p.makeFileRecord(ctx, d)
		if err != nil {
			return nil, err
		}
		fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
		if err != nil {
			return nil, err
		}
		if err := lookupAndSetFileMetadata(iter, fileMetadataKey, fileMetadata); err != nil {
			continue
		}
		rc, err := filestore.NewReader(ctx, blobDir, fileMetadata.GetStorageMetadata(), 0, 0)
		if err != nil {
			p.handleMetadataMismatch(err, fileMetadataKey, fileMetadata)
			continue
		}
		p.updateAtime(fileMetadataKey)

		_, copyErr := io.Copy(buf, rc)
		closeErr := rc.Close()
		if copyErr != nil || closeErr != nil {
			continue
		}
		foundMap[d] = append([]byte{}, buf.Bytes()...)
		buf.Reset()
	}
	return foundMap, nil
}

func (p *PebbleCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	wc, err := p.Writer(ctx, d)
	if err != nil {
		return err
	}
	if _, err := wc.Write(data); err != nil {
		return err
	}
	return wc.Close()
}

func (p *PebbleCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	for d, data := range kvs {
		if err := p.Set(ctx, d, data); err != nil {
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

func (p *PebbleCache) deleteMetadataOnly(fileMetadataKey []byte) error {
	iter := p.db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	// First, lookup the FileMetadata. If it's not found, we don't have the file.
	fileMetadata, err := lookupFileMetadata(iter, fileMetadataKey)
	if err != nil {
		return err
	}

	if err := p.db.Delete(fileMetadataKey, &pebble.WriteOptions{Sync: false}); err != nil {
		return err
	}
	p.clearAtime(fileMetadataKey)
	p.sendSizeUpdate(fileMetadata.GetFileRecord().GetIsolation().GetPartitionId(), fileMetadataKey, -1*fileMetadata.GetSizeBytes())
	return nil
}

func (p *PebbleCache) deleteRecord(ctx context.Context, fileMetadataKey []byte) error {
	iter := p.db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	// First, lookup the FileMetadata. If it's not found, we don't have the file.
	fileMetadata, err := lookupFileMetadata(iter, fileMetadataKey)
	if err != nil {
		return err
	}

	fp := filestore.FilePath(p.blobDir(), fileMetadata.GetStorageMetadata().GetFileMetadata())
	if err := p.db.Delete(fileMetadataKey, &pebble.WriteOptions{Sync: false}); err != nil {
		return err
	}
	p.clearAtime(fileMetadataKey)
	p.sendSizeUpdate(p.isolation.GetPartitionId(), fileMetadataKey, -1*fileMetadata.GetSizeBytes())
	return disk.DeleteFile(ctx, fp)
}

func (p *PebbleCache) Delete(ctx context.Context, d *repb.Digest) error {
	fileRecord, err := p.makeFileRecord(ctx, d)
	if err != nil {
		return err
	}
	fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
	if err != nil {
		return err
	}
	return p.deleteRecord(ctx, fileMetadataKey)
}

func (p *PebbleCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	iter := p.db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	fileRecord, err := p.makeFileRecord(ctx, d)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}

	// First, lookup the FileMetadata. If it's not found, we don't have the file.
	fileMetadata, err := lookupFileMetadata(iter, fileMetadataKey)
	if err != nil {
		return nil, err
	}

	rc, err := filestore.NewReader(ctx, p.blobDir(), fileMetadata.GetStorageMetadata(), offset, limit)
	if err == nil {
		p.updateAtime(fileMetadataKey)
	} else if status.IsNotFoundError(err) || os.IsNotExist(err) {
		p.handleMetadataMismatch(err, fileMetadataKey, fileMetadata)
	}
	return rc, err
}

type writeCloser struct {
	filestore.WriteCloserMetadata
	closeFn      func(n int64) error
	bytesWritten int64
}

func (dc *writeCloser) Close() error {
	if err := dc.WriteCloserMetadata.Close(); err != nil {
		return err
	}
	return dc.closeFn(dc.bytesWritten)
}

func (dc *writeCloser) Write(p []byte) (int, error) {
	n, err := dc.WriteCloserMetadata.Write(p)
	if err != nil {
		return 0, err
	}
	dc.bytesWritten += int64(n)
	return n, nil
}

func (p *PebbleCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	fileRecord, err := p.makeFileRecord(ctx, d)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}

	iter := p.db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()
	alreadyExists := hasFileMetadata(iter, fileMetadataKey)
	if alreadyExists {
		metrics.DiskCacheDuplicateWrites.Inc()
		metrics.DiskCacheDuplicateWritesBytes.Add(float64(d.GetSizeBytes()))
	}

	var wcm filestore.WriteCloserMetadata
	if d.GetSizeBytes() < *maxInlineFileSizeBytes {
		wcm = filestore.InlineWriter(ctx, d.GetSizeBytes())
	} else {
		fw, err := filestore.NewWriter(ctx, p.blobDir(), p.db.NewBatch(), fileRecord)
		if err != nil {
			return nil, err
		}
		wcm = fw
	}
	dc := &writeCloser{WriteCloserMetadata: wcm, closeFn: func(bytesWritten int64) error {
		md := &rfpb.FileMetadata{
			FileRecord:      fileRecord,
			StorageMetadata: wcm.Metadata(),
			SizeBytes:       bytesWritten,
		}
		protoBytes, err := proto.Marshal(md)
		if err != nil {
			return err
		}
		err = p.db.Set(fileMetadataKey, protoBytes, &pebble.WriteOptions{Sync: false})
		if err == nil {
			p.updateAtime(fileMetadataKey)
			p.sendSizeUpdate(p.isolation.GetPartitionId(), fileMetadataKey, bytesWritten)
			metrics.DiskCacheAddedFileSizeBytes.Observe(float64(bytesWritten))
		}
		return err
	}}
	return dc, nil
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

			if totalSizeBytes < int64(float64(maxAllowedSize)*.90) {
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
	mu      *sync.Mutex
	part    disk.Partition
	blobDir string
	reader  pebble.Reader
	writer  pebble.Writer
	atimes  *sync.Map

	casPrefix   []byte
	acPrefix    []byte
	samplePool  []*evictionPoolEntry
	sizeBytes   int64
	casCount    int64
	acCount     int64
	lastRun     time.Time
	lastEvicted *evictionPoolEntry
}

func newPartitionEvictor(part disk.Partition, blobDir string, db *pebble.DB, atimes *sync.Map) (*partitionEvictor, error) {
	pe := &partitionEvictor{
		mu:         &sync.Mutex{},
		part:       part,
		blobDir:    blobDir,
		casPrefix:  []byte(part.ID + "/cas/"),
		acPrefix:   []byte(part.ID + "/ac/"),
		samplePool: make([]*evictionPoolEntry, 0, samplePoolSize),
		reader:     db,
		writer:     db,
		atimes:     atimes,
	}
	start := time.Now()
	log.Printf("Pebble Cache: Initializing cache partition %q...", part.ID)
	sizeBytes, casCount, acCount, err := pe.computeSize()
	if err != nil {
		return nil, err
	}
	pe.sizeBytes = sizeBytes
	pe.casCount = casCount
	pe.acCount = acCount

	log.Printf("Pebble Cache: Initialized cache partition %q AC: %d, CAS: %d, Size: %d [bytes] in %s", part.ID, pe.acCount, pe.casCount, pe.sizeBytes, time.Since(start))
	return pe, nil
}

func (e *partitionEvictor) updateSize(fileMetadataKey []byte, deltaSize int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	deltaCount := int64(1)
	if deltaSize < 0 {
		deltaCount = -1
	}

	if bytes.Contains(fileMetadataKey, e.casPrefix) {
		e.casCount += deltaCount
	} else if bytes.Contains(fileMetadataKey, e.acPrefix) {
		e.acCount += deltaCount
	} else {
		log.Warningf("Unidentified file (not CAS or AC): %q", fileMetadataKey)
	}
	e.sizeBytes += deltaSize
}

func (e *partitionEvictor) computeSizeInRange(start, end []byte) (int64, int64, int64, error) {
	iter := e.reader.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
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
		blobSizeBytes += fileMetadata.GetSizeBytes()
		metadataSizeBytes += int64(len(iter.Value()))

		// identify and count CAS vs AC files.
		if bytes.Contains(iter.Key(), e.casPrefix) {
			casCount += 1
		} else if bytes.Contains(iter.Key(), e.acPrefix) {
			acCount += 1
		} else {
			log.Warningf("Unidentified file (not CAS or AC): %q", iter.Key())
		}
	}

	return blobSizeBytes + metadataSizeBytes, casCount, acCount, nil
}

func (e *partitionEvictor) computeSize() (int64, int64, int64, error) {
	mu := sync.Mutex{}
	eg := errgroup.Group{}

	totalSizeBytes := int64(0)
	totalCasCount := int64(0)
	totalAcCount := int64(0)

	goScanRange := func(start, end []byte) {
		eg.Go(func() error {
			sizeBytes, casCount, acCount, err := e.computeSizeInRange(start, end)
			if err != nil {
				return err
			}

			mu.Lock()
			totalSizeBytes += sizeBytes
			totalCasCount += casCount
			totalAcCount += acCount
			mu.Unlock()
			return nil
		})
	}

	acPrefixRange := func(start, end []byte) ([]byte, []byte) {
		left := make([]byte, 0, len(e.acPrefix)+len(start))
		left = append(left, e.acPrefix...)
		left = append(left, start...)

		right := make([]byte, 0, len(e.acPrefix)+len(end))
		right = append(right, e.acPrefix...)
		right = append(right, end...)
		return left, right
	}

	// Start scanning the AC.
	// AC keys look like /partitionID/ac/12312312313(crc-32)/digesthash
	// Start scanning at 10 because crc32s do not begin with 0.
	for i := 10; i < 99; i++ {
		left, right := acPrefixRange([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i+1)))
		goScanRange(left, right)
	}
	// Additionally scan from 99-> max byte to ensure we cover the full
	// range.
	left, right := acPrefixRange([]byte("99"), []byte{constants.MaxByte})
	goScanRange(left, right)

	// Start scanning the CAS.
	// CAS keys look like /partitionID/cas/digesthash(sha-256)
	for i := 0; i < 16; i++ {
		kr := append(e.casPrefix, []byte(fmt.Sprintf("%x", i))...)
		start, end := keyRange(kr)
		goScanRange(start, end)
	}

	if err := eg.Wait(); err != nil {
		return 0, 0, 0, err
	}
	return totalSizeBytes, totalCasCount, totalAcCount, nil
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
	buf += fmt.Sprintf("Usage: %d / %d (%2.2f%% full)\n", e.sizeBytes, maxAllowedSize, percentFull)
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

func (e *partitionEvictor) iter() *pebble.Iterator {
	start, end := partitionLimits(e.part.ID)
	iter := e.reader.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	iter.SeekGE(start)
	return iter
}

func getLastUse(info os.FileInfo) int64 {
	stat := info.Sys().(*syscall.Stat_t)
	// Super Gross! https://github.com/golang/go/issues/31735
	value := reflect.ValueOf(stat)
	var ts syscall.Timespec
	if timeField := value.Elem().FieldByName("Atimespec"); timeField.IsValid() {
		ts = timeField.Interface().(syscall.Timespec)
	} else if timeField := value.Elem().FieldByName("Atim"); timeField.IsValid() {
		ts = timeField.Interface().(syscall.Timespec)
	} else {
		ts = syscall.Timespec{}
	}
	t := time.Unix(ts.Sec, ts.Nsec)
	return t.UnixNano()
}

var digestRunes = []rune("abcdef1234567890")

func (e *partitionEvictor) randomKey(n int) []byte {
	randKey := e.part.ID
	totalCount := e.casCount + e.acCount

	randInt := rand.Int63n(totalCount)
	if randInt < e.casCount {
		randKey += "/cas/"
	} else {
		randKey += "/ac/"
	}
	for i := 0; i < n; i++ {
		randKey += string(digestRunes[rand.Intn(len(digestRunes))])
	}
	return []byte(randKey)
}

func (e *partitionEvictor) refreshAtime(s *evictionPoolEntry) error {
	if a, ok := e.atimes.Load(xxhash.Sum64(s.fileMetadataKey)); ok {
		s.timestamp = a.(int64)
		return nil
	}

	md := s.fileMetadata.GetStorageMetadata()
	switch {
	case md.GetFileMetadata() != nil:
		fp := filestore.FilePath(e.blobDir, md.GetFileMetadata())
		info, err := os.Stat(fp)
		if err != nil {
			return err
		}
		s.timestamp = getLastUse(info)
	case md.GetInlineMetadata() != nil:
		s.timestamp = md.GetInlineMetadata().GetCreatedAtNsec()
	}
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

func (e *partitionEvictor) deleteFile(sample *evictionPoolEntry) error {
	if err := e.writer.Delete(sample.fileMetadataKey, &pebble.WriteOptions{Sync: true}); err != nil {
		return err
	}
	e.atimes.Delete(xxhash.Sum64(sample.fileMetadataKey))

	md := sample.fileMetadata.GetStorageMetadata()
	switch {
	case md.GetFileMetadata() != nil:
		fp := filestore.FilePath(e.blobDir, md.GetFileMetadata())
		if err := disk.DeleteFile(context.TODO(), fp); err != nil {
			return err
		}
	case md.GetInlineMetadata() != nil:
		break
	default:
		return status.FailedPreconditionErrorf("Unnown storage metadata type: %+v", md)
	}

	ageUsec := float64(time.Since(time.Unix(0, sample.timestamp)).Microseconds())
	metrics.DiskCacheLastEvictionAgeUsec.With(prometheus.Labels{metrics.PartitionID: e.part.ID}).Set(ageUsec)
	e.updateSize(sample.fileMetadataKey, -1*sample.fileMetadata.GetSizeBytes())
	return nil
}

func (e *partitionEvictor) resampleK(k int) error {
	iter := e.iter()
	defer iter.Close()

	for i := 0; i < k; i++ {
		entries, err := e.randomSample(iter, sampleN)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			e.samplePool = append(e.samplePool, entry)
		}
	}

	for _, sample := range e.samplePool {
		if err := e.refreshAtime(sample); err != nil {
			continue
		}
	}

	if len(e.samplePool) > 0 {
		sort.Slice(e.samplePool, func(i, j int) bool {
			return e.samplePool[i].timestamp < e.samplePool[j].timestamp
		})
	}

	if len(e.samplePool) > samplePoolSize {
		e.samplePool = e.samplePool[:samplePoolSize]
	}

	return nil
}

func (e *partitionEvictor) evict(count int) (*evictionPoolEntry, error) {
	evicted := 0
	var lastEvicted *evictionPoolEntry
	for evicted < count {
		lastCount := evicted

		// Resample every time we evict a key
		if err := e.resampleK(1); err != nil {
			return nil, err
		}
		for i, sample := range e.samplePool {
			_, closer, err := e.reader.Get(sample.fileMetadataKey)
			if err == pebble.ErrNotFound {
				continue
			}
			closer.Close()
			log.Infof("Evictor %q attempting to delete file: %q", e.part.ID, sample.fileMetadataKey)
			if err := e.deleteFile(sample); err != nil {
				log.Errorf("Error evicting file: %s (ignoring)", err)
				continue
			}
			evicted += 1
			lastEvicted = sample
			e.samplePool = append(e.samplePool[:i], e.samplePool[i+1:]...)
			break
		}

		// If no candidates were evictable in the whole pool, resample
		// the pool.
		if lastCount == evicted {
			e.samplePool = e.samplePool[:0]
			if err := e.resampleK(samplePoolSize); err != nil {
				return nil, err
			}
		}
	}
	return lastEvicted, nil
}

func (e *partitionEvictor) ttl(quitChan chan struct{}) error {
	e.mu.Lock()
	maxAllowedSize := int64(JanitorCutoffThreshold * float64(e.part.MaxSizeBytes))
	e.mu.Unlock()

	for {
		e.mu.Lock()
		sizeBytes := e.sizeBytes
		totalCount := e.casCount + e.acCount
		e.mu.Unlock()

		if sizeBytes < int64(float64(maxAllowedSize)*.90) {
			break
		}

		select {
		case <-quitChan:
			return nil
		default:
			break
		}

		numToEvict := int(.001 * float64(totalCount))
		if numToEvict == 0 {
			numToEvict = 1
		}

		lastEvicted, err := e.evict(numToEvict)
		if err != nil {
			return err
		}

		e.mu.Lock()
		e.lastRun = time.Now()
		e.lastEvicted = lastEvicted
		e.mu.Unlock()
	}
	return nil
}

func (e *partitionEvictor) run(quitChan chan struct{}) error {
	for {
		select {
		case <-quitChan:
			return nil
		case <-time.After(JanitorCheckPeriod):
			if err := e.ttl(quitChan); err != nil {
				return err
			}
		}
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
		p.processSizeUpdates(p.quitChan)
		return nil
	})
	p.eg.Go(func() error {
		p.scanForBrokenFiles(p.quitChan)
		return nil
	})
	return nil
}

func (p *PebbleCache) Stop() error {
	close(p.quitChan)
	if err := p.eg.Wait(); err != nil {
		return err
	}
	if err := p.db.Flush(); err != nil {
		return err
	}

	// TODO(tylerw) re-enable this once shutdown race is debugged.
	// return p.db.Close()
	return nil
}
