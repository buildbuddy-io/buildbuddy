package disk_cache

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

const (
	// cutoffThreshold is the point above which a janitor thread will run
	// and delete the oldest items from the cache.
	janitorCutoffThreshold = .90

	// janitorCheckPeriod is how often the janitor thread will wake up to
	// check the cache size.
	janitorCheckPeriod = 100 * time.Millisecond

	// staleFilePeriod is the timeframe after which an empty or temp file will be considered stale and eligible
	// for automatic deletion
	staleFilePeriod = 15 * time.Minute

	DefaultPartitionID       = "default"
	PartitionDirectoryPrefix = "PT"
	HashPrefixDirPrefixLen   = 4
	V2Dir                    = "v2"
)

var (
	rootDirectoryFlag     = flag.String("cache.disk.root_directory", "", "The root directory to store all blobs in, if using disk based storage.")
	partitionsFlag        = flagutil.New("cache.disk.partitions", []disk.Partition{}, "")
	partitionMappingsFlag = flagutil.New("cache.disk.partition_mappings", []disk.PartitionMapping{}, "")
	useV2LayoutFlag       = flag.Bool("cache.disk.use_v2_layout", false, "If enabled, files will be stored using the v2 layout. See disk_cache.MigrateToV2Layout for a description.")

	migrateDiskCacheToV2AndExit = flag.Bool("migrate_disk_cache_to_v2_and_exit", false, "If true, attempt to migrate disk cache to v2 layout.")
)

type Options struct {
	RootDirectory     string
	Partitions        []disk.Partition
	PartitionMappings []disk.PartitionMapping
	UseV2Layout       bool
}

// MigrateToV2Layout restructures the files under the root directory to conform to the "v2" layout.
// Difference between v1 and v2:
//  - Digests are now placed under a subdirectory based on the first 4 characters of the hash to avoid directories
//    with large number of files.
//  - Files in the default partition are now placed under a "PTdefault" partition subdirectory to be consistent with
//    other partitions.
// TODO(vadim): make this automatic so we can migrate onprem customers
func MigrateToV2Layout(rootDir string) error {
	log.Info("Starting digest migration.")
	numMigrated := 0
	start := time.Now()
	var dirsToDelete []string
	walkFn := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(rootDir, path)
		if err != nil {
			return status.InternalErrorf("Could not compute %q relative to %q", path, rootDir)
		}
		if d.IsDir() {
			// Don't need to do anything for files already under the v2 directory.
			if relPath == V2Dir {
				return filepath.SkipDir
			}
			if path != rootDir {
				dirsToDelete = append(dirsToDelete, path)
			}
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		if len(info.Name()) < HashPrefixDirPrefixLen {
			return status.InternalErrorf("File %q is an invalid digest", path)
		}

		newRoot := filepath.Join(rootDir, V2Dir)
		if !strings.HasPrefix(relPath, PartitionDirectoryPrefix) {
			newRoot = filepath.Join(newRoot, PartitionDirectoryPrefix+DefaultPartitionID)
		}

		newDir := filepath.Join(newRoot, filepath.Dir(relPath), info.Name()[0:HashPrefixDirPrefixLen])
		newPath := filepath.Join(newDir, info.Name())

		if err := os.MkdirAll(newDir, 0755); err != nil {
			return status.InternalErrorf("Could not create destination dir %q: %s", newDir, err)
		}

		if err := os.Rename(path, newPath); err != nil {
			return status.InternalErrorf("Could not rename %q -> %q: %s", path, newPath, err)
		}

		numMigrated++
		if numMigrated%1_000_000 == 0 {
			log.Infof("Migrated %d files in %s.", numMigrated, time.Since(start))
			log.Infof("Most recent migration: %q -> %q", path, newPath)
		}

		return nil
	}
	if err := filepath.WalkDir(rootDir, walkFn); err != nil {
		return err
	}
	for i := len(dirsToDelete) - 1; i >= 0; i-- {
		if err := os.Remove(dirsToDelete[i]); err != nil {
			log.Warningf("Could not delete directory %q: %s", dirsToDelete[i], err)
		}
	}
	log.Infof("Migrated %d digests in %s.", numMigrated, time.Since(start))
	return nil
}

// DiskCache stores data on disk as files.
// It is broken up into partitions which are independent and maintain their own LRUs.
type DiskCache struct {
	env               environment.Env
	partitions        map[string]*partition
	partitionMappings []disk.PartitionMapping
	defaultPartition  *partition

	// TODO(Maggie): Deprecate these fields
	// The currently selected partition. Initialized to the default partition.
	// WithRemoteInstanceName can create a new cache accessor with a different selected partition.
	partition          *partition
	cacheType          resource.CacheType
	remoteInstanceName string
}

func Register(env environment.Env) error {
	if *rootDirectoryFlag == "" {
		return nil
	}
	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with disk_cache.")
	}
	dc := &Options{
		RootDirectory:     *rootDirectoryFlag,
		Partitions:        *partitionsFlag,
		PartitionMappings: *partitionMappingsFlag,
		UseV2Layout:       *useV2LayoutFlag,
	}
	c, err := NewDiskCache(env, dc, cache_config.MaxSizeBytes())
	if err != nil {
		return status.InternalErrorf("Error configuring cache: %s", err)
	}
	env.SetCache(c)
	return nil
}

func NewDiskCache(env environment.Env, opts *Options, defaultMaxSizeBytes int64) (*DiskCache, error) {
	if opts.RootDirectory == "" {
		return nil, status.FailedPreconditionError("Disk cache root directory must be set")
	}

	if *migrateDiskCacheToV2AndExit {
		if err := MigrateToV2Layout(opts.RootDirectory); err != nil {
			log.Errorf("Migration failed: %s", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	c := &DiskCache{
		env:                env,
		partitionMappings:  opts.PartitionMappings,
		cacheType:          resource.CacheType_CAS,
		remoteInstanceName: "",
	}

	partitions := make(map[string]*partition)
	var defaultPartition *partition
	for _, pc := range opts.Partitions {
		rootDir := opts.RootDirectory
		if opts.UseV2Layout {
			rootDir = filepath.Join(rootDir, V2Dir)
		}

		if pc.ID != DefaultPartitionID || opts.UseV2Layout {
			if pc.ID == "" {
				return nil, status.InvalidArgumentError("Non-default partition %q must have a valid ID")
			}
			rootDir = filepath.Join(rootDir, PartitionDirectoryPrefix+pc.ID)
		}

		p, err := newPartition(pc.ID, rootDir, pc.MaxSizeBytes, opts.UseV2Layout)
		if err != nil {
			return nil, err
		}
		partitions[pc.ID] = p
		if pc.ID == DefaultPartitionID {
			defaultPartition = p
		}
	}
	if defaultPartition == nil {
		rootDir := opts.RootDirectory
		if opts.UseV2Layout {
			rootDir = filepath.Join(rootDir, V2Dir, PartitionDirectoryPrefix+DefaultPartitionID)
		}
		p, err := newPartition(DefaultPartitionID, rootDir, defaultMaxSizeBytes, opts.UseV2Layout)
		if err != nil {
			return nil, err
		}
		defaultPartition = p
		partitions[DefaultPartitionID] = p
	}

	c.partitions = partitions
	c.partition = defaultPartition
	c.defaultPartition = defaultPartition

	statusz.AddSection("disk_cache", "On disk LRU cache", c)
	return c, nil
}

func (c *DiskCache) getPartition(ctx context.Context, remoteInstanceName string) (*partition, error) {
	auth := c.env.GetAuthenticator()
	if auth == nil {
		return c.defaultPartition, nil
	}
	user, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return c.defaultPartition, nil
	}
	for _, m := range c.partitionMappings {
		if m.GroupID == user.GetGroupID() && strings.HasPrefix(remoteInstanceName, m.Prefix) {
			p, ok := c.partitions[m.PartitionID]
			if !ok {
				return nil, status.NotFoundErrorf("Mapping to unknown partition %q", m.PartitionID)
			}
			return p, nil
		}
	}
	return c.defaultPartition, nil
}

func (c *DiskCache) WithIsolation(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	p, err := c.getPartition(ctx, remoteInstanceName)
	if err != nil {
		return nil, err
	}

	return &DiskCache{
		env:                c.env,
		partition:          p,
		defaultPartition:   c.defaultPartition,
		partitions:         c.partitions,
		partitionMappings:  c.partitionMappings,
		cacheType:          cacheType,
		remoteInstanceName: remoteInstanceName,
	}, nil
}

func (c *DiskCache) Statusz(ctx context.Context) string {
	buf := ""
	for _, p := range c.partitions {
		buf += p.Statusz(ctx)
	}
	return buf
}

func (c *DiskCache) Contains(ctx context.Context, r *resource.ResourceName) (bool, error) {
	p, err := c.getPartition(ctx, r.GetInstanceName())
	if err != nil {
		return false, err
	}

	record, err := p.lruGet(ctx, r.GetCacheType(), r.GetInstanceName(), r.GetDigest())
	contains := record != nil
	return contains, err
}

func (c *DiskCache) ContainsDeprecated(ctx context.Context, d *repb.Digest) (bool, error) {
	return c.Contains(ctx, &resource.ResourceName{
		Digest:       d,
		InstanceName: c.remoteInstanceName,
		Compressor:   repb.Compressor_IDENTITY,
		CacheType:    c.cacheType,
	})
}

func (c *DiskCache) Metadata(ctx context.Context, r *resource.ResourceName) (*interfaces.CacheMetadata, error) {
	p, err := c.getPartition(ctx, r.GetInstanceName())
	if err != nil {
		return nil, err
	}

	d := r.GetDigest()
	lruRecord, err := p.lruGet(ctx, r.GetCacheType(), r.GetInstanceName(), d)
	if err != nil {
		return nil, err
	}
	if lruRecord == nil {
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}

	fileInfo, err := os.Stat(lruRecord.FullPath())
	if err != nil {
		return nil, err
	}

	lastUseNanos := getLastUseNanos(fileInfo)
	lastModifyNanos := getLastModifyTimeNanos(fileInfo)

	return &interfaces.CacheMetadata{
		SizeBytes:          fileInfo.Size(),
		LastAccessTimeUsec: lastUseNanos / 1000,
		LastModifyTimeUsec: lastModifyNanos / 1000,
	}, nil
}

func (c *DiskCache) MetadataDeprecated(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	return c.Metadata(ctx, &resource.ResourceName{
		Digest:       d,
		InstanceName: c.remoteInstanceName,
		Compressor:   repb.Compressor_IDENTITY,
		CacheType:    c.cacheType,
	})
}

func (c *DiskCache) FindMissing(ctx context.Context, resources []*resource.ResourceName) ([]*repb.Digest, error) {
	if len(resources) == 0 {
		return nil, nil
	}
	remoteInstanceName := resources[0].GetInstanceName()

	p, err := c.getPartition(ctx, remoteInstanceName)
	if err != nil {
		return nil, err
	}

	return p.findMissing(ctx, resources)
}

func (c *DiskCache) FindMissingDeprecated(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	rns := digest.ResourceNames(c.cacheType, c.remoteInstanceName, digests)
	return c.FindMissing(ctx, rns)
}

func (c *DiskCache) Get(ctx context.Context, r *resource.ResourceName) ([]byte, error) {
	p, err := c.getPartition(ctx, r.GetInstanceName())
	if err != nil {
		return nil, err
	}
	return p.get(ctx, r.GetCacheType(), r.GetInstanceName(), r.GetDigest())
}

func (c *DiskCache) GetDeprecated(ctx context.Context, d *repb.Digest) ([]byte, error) {
	return c.partition.get(ctx, c.cacheType, c.remoteInstanceName, d)
}

func (c *DiskCache) GetMulti(ctx context.Context, resources []*resource.ResourceName) (map[*repb.Digest][]byte, error) {
	if len(resources) == 0 {
		return nil, nil
	}
	remoteInstanceName := resources[0].GetInstanceName()
	p, err := c.getPartition(ctx, remoteInstanceName)
	if err != nil {
		return nil, err
	}
	return p.getMulti(ctx, resources)
}

func (c *DiskCache) GetMultiDeprecated(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	rns := digest.ResourceNames(c.cacheType, c.remoteInstanceName, digests)
	return c.partition.getMulti(ctx, rns)
}

func (c *DiskCache) Set(ctx context.Context, r *resource.ResourceName, data []byte) error {
	p, err := c.getPartition(ctx, r.GetInstanceName())
	if err != nil {
		return err
	}
	return p.set(ctx, r.GetCacheType(), r.GetInstanceName(), r.GetDigest(), data)
}

func (c *DiskCache) SetDeprecated(ctx context.Context, d *repb.Digest, data []byte) error {
	r := &resource.ResourceName{
		Digest:       d,
		InstanceName: c.remoteInstanceName,
		Compressor:   repb.Compressor_IDENTITY,
		CacheType:    c.cacheType,
	}
	return c.Set(ctx, r, data)
}

func (c *DiskCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	return c.partition.setMulti(ctx, c.cacheType, c.remoteInstanceName, kvs)
}

func (c *DiskCache) Delete(ctx context.Context, d *repb.Digest) error {
	return c.partition.delete(ctx, c.cacheType, c.remoteInstanceName, d)
}

func (c *DiskCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	return c.partition.reader(ctx, c.cacheType, c.remoteInstanceName, d, offset, limit)
}

func (c *DiskCache) Writer(ctx context.Context, d *repb.Digest) (interfaces.CommittedWriteCloser, error) {
	return c.partition.writer(ctx, c.cacheType, c.remoteInstanceName, d)
}

func (c *DiskCache) WaitUntilMapped() {
	for _, p := range c.partitions {
		p.WaitUntilMapped()
	}
}

// We keep a record (in memory) of file atime (Last Access Time) and size, and
// when our cache reaches maxSize we remove the oldest files. Rather than
// serialize this ledger, we regenerate it from scratch on startup by looking
// at the filesystem.
type partition struct {
	id               string
	useV2Layout      bool
	mu               sync.RWMutex
	rootDir          string
	maxSizeBytes     int64
	lru              interfaces.LRU
	fileChannel      chan *fileRecord
	diskIsMapped     bool
	doneAsyncLoading chan struct{}
	lastGCTime       time.Time
	stringLock       sync.RWMutex
	internedStrings  map[string]string
}

func newPartition(id string, rootDir string, maxSizeBytes int64, useV2Layout bool) (*partition, error) {
	p := &partition{
		id:               id,
		useV2Layout:      useV2Layout,
		maxSizeBytes:     maxSizeBytes,
		rootDir:          rootDir,
		fileChannel:      make(chan *fileRecord),
		internedStrings:  make(map[string]string, 0),
		doneAsyncLoading: make(chan struct{}),
	}
	l, err := lru.NewLRU(&lru.Config{MaxSize: maxSizeBytes, OnEvict: p.evictFn, SizeFn: sizeFn})
	if err != nil {
		return nil, err
	}
	p.lru = l
	if err := p.initializeCache(); err != nil {
		return nil, err
	}
	p.startJanitor()
	return p, nil
}

// fileRecord is the data struct we store in the LRU cache
type fileRecord struct {
	key *fileKey
	// Size of the data on disk. Multiple of filesystem block size.
	// NOT the actual size of the digest.
	sizeOnDiskBytes int64
}

// timestampedFileRecord is a wrapper for fileRecord that contains additional metadata that does not need to be
// written in the value field to the LRU cache
type timestampedFileRecord struct {
	*fileRecord
	lastUseNanos int64
}

func (fr *fileRecord) FullPath() string {
	return fr.key.FullPath()
}

func sizeFn(value interface{}) int64 {
	size := int64(0)
	if v, ok := value.(*fileRecord); ok {
		size += v.sizeOnDiskBytes
	}
	return size
}

func getLastUseNanos(info os.FileInfo) int64 {
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
	return time.Unix(ts.Sec, ts.Nsec).UnixNano()
}

func getLastModifyTimeNanos(info os.FileInfo) int64 {
	stat := info.Sys().(*syscall.Stat_t)
	// Super Gross! https://github.com/golang/go/issues/31735
	value := reflect.ValueOf(stat)
	var ts syscall.Timespec
	if timeField := value.Elem().FieldByName("Mtimespec"); timeField.IsValid() {
		ts = timeField.Interface().(syscall.Timespec)
	} else if timeField := value.Elem().FieldByName("Mtim"); timeField.IsValid() {
		ts = timeField.Interface().(syscall.Timespec)
	} else {
		ts = syscall.Timespec{}
	}
	return time.Unix(ts.Sec, ts.Nsec).UnixNano()
}

func makeRecordFromPath(key *fileKey, fullPath string) (*fileRecord, error) {
	info, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}
	return makeRecordFromInfo(key, info), nil
}

func makeRecordFromInfo(key *fileKey, info fs.FileInfo) *fileRecord {
	stat := info.Sys().(*syscall.Stat_t)
	// Stat always returns number of 512 byte blocks, regardless of the FS
	// settings.
	sizeOnDisk := stat.Blocks * 512
	return &fileRecord{
		key:             key,
		sizeOnDiskBytes: sizeOnDisk,
	}
}

func (p *partition) evictFn(value interface{}) {
	if v, ok := value.(*fileRecord); ok {
		i, err := os.Stat(v.FullPath())
		if err == nil {
			lastUse := time.Unix(0, getLastUseNanos(i))
			ageUsec := float64(time.Now().Sub(lastUse).Microseconds())
			metrics.DiskCacheLastEvictionAgeUsec.With(prometheus.Labels{metrics.PartitionID: p.id}).Set(ageUsec)
		}
		if err := disk.DeleteFile(context.TODO(), v.FullPath()); err != nil {
			log.Warningf("Could delete evicted file: %s", err)
		}
	}
}

func (p *partition) internString(s string) string {
	p.stringLock.RLock()
	v, ok := p.internedStrings[s]
	p.stringLock.RUnlock()
	if ok {
		return v
	}
	p.stringLock.Lock()
	p.internedStrings[s] = s
	p.stringLock.Unlock()
	return s
}

func (p *partition) makeTimestampedRecordFromPathAndFileInfo(fullPath string, info os.FileInfo) (*timestampedFileRecord, error) {
	fk := &fileKey{}
	if err := fk.FromPartitionAndPath(p, fullPath); err != nil {
		return nil, err
	}
	return &timestampedFileRecord{
		fileRecord:   makeRecordFromInfo(fk, info),
		lastUseNanos: getLastUseNanos(info),
	}, nil
}

func (p *partition) WaitUntilMapped() {
	<-p.doneAsyncLoading
}

func (p *partition) Statusz(ctx context.Context) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	buf := "<br>"
	buf += fmt.Sprintf("<div>Partition %q</div>", p.id)
	buf += fmt.Sprintf("<div>Root directory: %s</div>", p.rootDir)
	percentFull := float64(p.lru.Size()) / float64(p.maxSizeBytes) * 100.0
	buf += fmt.Sprintf("<div>Capacity: %d / %d (%2.2f%% full)</div>", p.lru.Size(), p.maxSizeBytes, percentFull)
	buf += fmt.Sprintf("<div>Mapped into LRU: %t</div>", p.diskIsMapped)
	buf += fmt.Sprintf("<div>GC Last run: %s</div>", p.lastGCTime.Format("Jan 02, 2006 15:04:05 MST"))
	return buf
}

func (p *partition) reduceCacheSize(targetSize int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lru.Size() < targetSize {
		return false
	}
	value, ok := p.lru.RemoveOldest()
	if !ok {
		return false // should never happen
	}
	if fr, ok := value.(*fileRecord); ok {
		log.Debugf("Delete thread removed item from cache with key %v.", fr.key)
	}
	p.lastGCTime = time.Now()
	return true
}

func (p *partition) startJanitor() {
	targetSize := int64(float64(p.maxSizeBytes) * janitorCutoffThreshold)
	go func() {
		for {
			select {
			case <-time.After(janitorCheckPeriod):
				for {
					if !p.reduceCacheSize(targetSize) {
						break
					}
				}
			}
		}
	}()
}

func (p *partition) initializeCache() error {
	if err := disk.EnsureDirectoryExists(p.rootDir); err != nil {
		return err
	}

	go func() {
		start := time.Now()
		timestampedRecords := make([]*timestampedFileRecord, 0)
		inFlightRecords := make([]*fileRecord, 0)
		finishedFileChannel := make(chan struct{})
		go func() {
			for record := range p.fileChannel {
				inFlightRecords = append(inFlightRecords, record)
			}
			close(finishedFileChannel)
		}()
		walkFn := func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				// Originally there was just one "partition" with its contents under the root directory.
				// Additional partition directories live under the root as well and they need to be ignored
				// when initializing the default partition.
				if !p.useV2Layout && p.id == DefaultPartitionID && strings.HasPrefix(d.Name(), PartitionDirectoryPrefix) {
					return filepath.SkipDir
				}
				return nil
			}

			info, err := d.Info()
			if err != nil {
				// File disappeared since the directory entries were read.
				// We handle streamed writes by writing to a temp file & then renaming it so it's possible that
				// we can come across some temp files that disappear.
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if info.Size() == 0 {
				if isStaleFile(info) {
					log.Debugf("Deleting 0 length file: %q", path)
					err = os.Remove(path)
					if err != nil {
						log.Warningf("Could not delete 0 length file %q: %s", path, err)
					}
				}
				return nil
			}
			timestampedRecord, err := p.makeTimestampedRecordFromPathAndFileInfo(path, info)
			if err != nil {
				if disk.IsWriteTempFile(path) {
					if isStaleFile(info) {
						err = os.Remove(path)
						if err != nil {
							log.Warningf("Could not delete stale temp file %q (size %d): %s", path, info.Size(), err)
						}
					}
					return nil
				}
				log.Infof("Skipping unrecognized file %q (size %d): %s", path, info.Size(), err)
				return nil
			}
			timestampedRecords = append(timestampedRecords, timestampedRecord)
			if len(timestampedRecords)%1e6 == 0 {
				log.Printf("Disk Cache: progress: scanned %d files in %s...", len(timestampedRecords), time.Since(start))
			}
			return nil
		}
		if err := filepath.WalkDir(p.rootDir, walkFn); err != nil {
			alert.UnexpectedEvent("disk_cache_error_walking_directory", "err: %s", err)
		}

		// Sort entries by descending ATime.
		sort.Slice(timestampedRecords, func(i, j int) bool { return timestampedRecords[i].lastUseNanos > timestampedRecords[j].lastUseNanos })

		p.mu.Lock()
		// Populate our LRU with everything we scanned from disk, until the LRU reaches capacity.
		for _, timestampedRecord := range timestampedRecords {
			record := timestampedRecord.fileRecord
			if added := p.lru.PushBack(record.FullPath(), record); !added {
				break
			}
		}

		// Add in-flight records to the LRU. These were new files
		// touched during the loading phase, so we assume they are new
		// enough to just add to the top of the LRU without sorting.
		close(p.fileChannel)
		<-finishedFileChannel
		for _, record := range inFlightRecords {
			p.lruAdd(record)
		}
		inFlightRecords = nil
		log.Infof("DiskCache partition %q: loaded %d files in %s", p.id, len(timestampedRecords), time.Since(start))
		log.Infof("Finished initializing disk cache partition %q at %q. Current size: %d (max: %d) bytes", p.id, p.rootDir, p.lru.Size(), p.maxSizeBytes)

		p.diskIsMapped = true
		close(p.doneAsyncLoading)
		p.mu.Unlock()
	}()
	return nil
}

func isStaleFile(info fs.FileInfo) bool {
	return time.Since(info.ModTime()) > staleFilePeriod
}

func parseFilePath(rootDir, fullPath string, useV2Layout bool) (cacheType resource.CacheType, userPrefix, remoteInstanceName string, digestBytes []byte, err error) {
	p := strings.TrimPrefix(fullPath, rootDir+"/")
	parts := strings.Split(p, "/")

	parseError := func() error {
		return status.FailedPreconditionErrorf("file %q did not match expected format.", fullPath)
	}

	// pull group off the front
	if len(parts) > 0 {
		userPrefix = parts[0]
		parts = parts[1:]
	} else {
		err = parseError()
		return
	}

	// pull digest off the end
	if len(parts) > 0 {
		db, decodeErr := hex.DecodeString(parts[len(parts)-1])
		if decodeErr != nil {
			err = parseError()
			return
		}
		digestBytes = db
		parts = parts[:len(parts)-1]
	} else {
		err = parseError()
		return
	}

	// If this is a v2-formatted path, remove the hash prefix dir
	if useV2Layout {
		if len(parts) > 0 {
			prefixPart := parts[len(parts)-1]
			if len(prefixPart) != HashPrefixDirPrefixLen {
				err = parseError()
				return
			}
			parts = parts[:len(parts)-1]
		} else {
			err = parseError()
			return
		}
	}

	// If this is an /ac/ directory, this was an actionCache item
	// otherwise it was a CAS item.
	if len(parts) > 0 && parts[len(parts)-1] == "ac" {
		cacheType = resource.CacheType_AC
		parts = parts[:len(parts)-1]
	} else {
		cacheType = resource.CacheType_CAS
	}

	if len(parts) > 0 {
		remoteInstanceName = strings.Join(parts, "/")
	}
	return
}

func ScanDiskDirectory(scanDir string) <-chan *rfpb.FileMetadata {
	scanned := make(chan *rfpb.FileMetadata, 10)
	walkFn := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil // keep going
		}
		rootDir := scanDir
		relPath, err := filepath.Rel(rootDir, path)
		if err != nil {
			return status.InternalErrorf("Could not compute %q relative to %q", path, rootDir)
		}
		useV2Layout := strings.HasPrefix(relPath, V2Dir)
		if useV2Layout {
			rootDir = rootDir + "/" + V2Dir
			relPath = strings.TrimPrefix(relPath, V2Dir+"/")
		}

		partID := DefaultPartitionID
		if strings.HasPrefix(relPath, PartitionDirectoryPrefix) {
			relPathDirs := strings.SplitN(relPath, string(filepath.Separator), 2)
			partID = strings.TrimPrefix(relPathDirs[0], PartitionDirectoryPrefix)
			rootDir = rootDir + "/" + relPathDirs[0]
		}
		info, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.Size() == 0 {
			log.Debugf("Skipping 0 length file: %q", path)
			return nil
		}
		cacheType, _, remoteInstanceName, digestBytes, err := parseFilePath(rootDir, path, useV2Layout)
		if err != nil {
			log.Debugf("Skipping unparsable file: %s", err)
			return nil
		}

		isolation := &rfpb.Isolation{}
		isolation.CacheType = cacheType
		isolation.RemoteInstanceName = remoteInstanceName
		isolation.PartitionId = partID
		fm := &rfpb.FileMetadata{
			FileRecord: &rfpb.FileRecord{
				Isolation: isolation,
				Digest: &repb.Digest{
					Hash:      hex.EncodeToString(digestBytes),
					SizeBytes: info.Size(),
				},
			},
			StorageMetadata: &rfpb.StorageMetadata{
				FileMetadata: &rfpb.StorageMetadata_FileMetadata{
					Filename: path,
				},
			},
			SizeBytes:      info.Size(),
			LastAccessUsec: getLastUseNanos(info),
			LastModifyUsec: getLastModifyTimeNanos(info),
		}
		scanned <- fm
		return nil
	}
	go func() {
		if err := filepath.WalkDir(scanDir, walkFn); err != nil {
			alert.UnexpectedEvent("disk_cache_error_walking_directory", "err: %s", err)
		}
		close(scanned)
	}()
	return scanned
}

type fileKey struct {
	part               *partition
	cacheType          resource.CacheType
	userPrefix         string
	remoteInstanceName string
	digestBytes        []byte
}

func (fk *fileKey) FromPartitionAndPath(part *partition, fullPath string) error {
	fk.part = part

	cacheType, userPrefix, remoteInstanceName, digestBytes, err := parseFilePath(fk.part.rootDir, fullPath, fk.part.useV2Layout)
	if err != nil {
		return err
	}

	fk.userPrefix = fk.part.internString(userPrefix)
	fk.digestBytes = digestBytes
	fk.cacheType = cacheType
	fk.remoteInstanceName = fk.part.internString(remoteInstanceName)

	return nil
}

func (fk *fileKey) FullPath() string {
	hashPrefixDir := ""
	digestHash := hex.EncodeToString(fk.digestBytes)
	if fk.part.useV2Layout {
		hashPrefixDir = digestHash[0:HashPrefixDirPrefixLen] + "/"
	}
	return filepath.Join(fk.part.rootDir, fk.userPrefix, fk.remoteInstanceName, digest.CacheTypeToPrefix(fk.cacheType), hashPrefixDir+digestHash)
}

func (p *partition) key(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string, d *repb.Digest) (*fileKey, error) {
	hash, err := digest.Validate(d)
	if err != nil {
		return nil, err
	}
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if len(hash) < HashPrefixDirPrefixLen {
		alert.UnexpectedEvent("disk_cache_digest_too_short", "digest hash %q is too short", hash)
		return nil, status.FailedPreconditionErrorf("digest hash %q is way too short!", hash)
	}

	digestBytes, err := hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}
	return &fileKey{
		part:               p,
		cacheType:          cacheType,
		userPrefix:         p.internString(userPrefix),
		remoteInstanceName: p.internString(remoteInstanceName),
		digestBytes:        digestBytes,
	}, nil
}

func (p *partition) lruAdd(record *fileRecord) {
	p.lru.Add(record.FullPath(), record)
}

// Adds a single file, using the provided path, to the LRU.
// NB: Callers are responsible for locking the LRU before calling this function.
func (p *partition) addFileToLRUIfExists(key *fileKey) *fileRecord {
	if p.diskIsMapped {
		return nil
	}
	info, err := os.Stat(key.FullPath())
	if err == nil {
		if info.Size() == 0 {
			log.Debugf("Skipping 0 length file: %q", key.FullPath())
			return nil
		}
		record := makeRecordFromInfo(key, info)
		p.fileChannel <- record
		p.lruAdd(record)
		return record
	}
	return nil
}

func (p *partition) lruGet(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string, d *repb.Digest) (*fileRecord, error) {
	k, err := p.key(ctx, cacheType, remoteInstanceName, d)
	if err != nil {
		return nil, err
	}
	// Bazel does frequent "contains" checks, so we want to make this fast.
	// We could check the disk and see if the file exists, because it's
	// possible the file has been deleted out from under us, but in the
	// interest of performance we just check our local records.

	// Why do we "use" the entry? (AKA mark it as not ready for eviction)
	// From the protocol description:
	// Implementations SHOULD ensure that any blobs referenced from the
	// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
	// are available at the time of returning the
	// [ActionResult][build.bazel.remote.execution.v2.ActionResult] and will be
	// for some period of time afterwards. The TTLs of the referenced blobs SHOULD be increased
	// if necessary and applicable.
	p.mu.Lock()
	defer p.mu.Unlock()
	v, ok := p.lru.Get(k.FullPath())
	if ok {
		vr, ok := v.(*fileRecord)
		if !ok {
			return nil, status.InternalErrorf("not a *fileRecord")
		}
		return vr, nil
	}
	if !p.diskIsMapped {
		// OK if we're here it means the disk contents are still being loaded
		// into the LRU. But we still need to return an answer! So we'll go
		// check the FS, and if the file is there we'll add it to the LRU.
		lruRecord := p.addFileToLRUIfExists(k)
		return lruRecord, nil
	}
	return nil, nil
}

func (p *partition) findMissing(ctx context.Context, resources []*resource.ResourceName) ([]*repb.Digest, error) {
	lock := sync.RWMutex{} // protects(missing)
	var missing []*repb.Digest
	eg, ctx := errgroup.WithContext(ctx)

	for _, r := range resources {
		fetchFn := func(r *resource.ResourceName) {
			eg.Go(func() error {
				record, err := p.lruGet(ctx, r.GetCacheType(), r.GetInstanceName(), r.GetDigest())
				// NotFoundError is never returned from lruGet above, so
				// we don't check for it.
				if err != nil {
					return err
				}
				if record == nil {
					lock.Lock()
					missing = append(missing, r.GetDigest())
					lock.Unlock()
				}
				return nil
			})
		}
		fetchFn(r)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return missing, nil
}

func (p *partition) get(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string, d *repb.Digest) ([]byte, error) {
	k, err := p.key(ctx, cacheType, remoteInstanceName, d)
	if err != nil {
		return nil, err
	}
	buf, err := disk.ReadFile(ctx, k.FullPath())
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.lru.Remove(k.FullPath()) // remove it just in case
		return nil, status.NotFoundErrorf("DiskCache missing file: %s", err)
	}

	if p.lru.Contains(k.FullPath()) {
		p.lru.Get(k.FullPath()) // mark the file as used.
	} else if !p.diskIsMapped {
		p.addFileToLRUIfExists(k)
	}
	return buf, nil
}

func (p *partition) getMulti(ctx context.Context, resources []*resource.ResourceName) (map[*repb.Digest][]byte, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest][]byte, len(resources))
	eg, ctx := errgroup.WithContext(ctx)

	for _, r := range resources {
		fetchFn := func(r *resource.ResourceName) {
			eg.Go(func() error {
				d := r.GetDigest()
				data, err := p.get(ctx, r.GetCacheType(), r.GetInstanceName(), d)
				if status.IsNotFoundError(err) {
					return nil
				}
				if err != nil {
					return err
				}
				lock.Lock()
				foundMap[d] = data
				lock.Unlock()
				return nil
			})
		}
		fetchFn(r)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return foundMap, nil
}

func (p *partition) set(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string, d *repb.Digest, data []byte) error {
	k, err := p.key(ctx, cacheType, remoteInstanceName, d)
	if err != nil {
		return err
	}
	n, err := disk.WriteFile(ctx, k.FullPath(), data)
	if err != nil {
		// If we had an error writing the file, just return that.
		return err
	}
	record, err := makeRecordFromPath(k, k.FullPath())
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.lruAdd(record)
	metrics.DiskCacheAddedFileSizeBytes.Observe(float64(n))
	return err
}

func (p *partition) setMulti(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string, kvs map[*repb.Digest][]byte) error {
	for d, data := range kvs {
		if err := p.set(ctx, cacheType, remoteInstanceName, d, data); err != nil {
			return err
		}
	}
	return nil
}

func (p *partition) delete(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string, d *repb.Digest) error {
	k, err := p.key(ctx, cacheType, remoteInstanceName, d)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	removed := p.lru.Remove(k.FullPath())
	if !removed {
		return status.NotFoundErrorf("digest %s/%d not found in disk cache", d.GetHash(), d.GetSizeBytes())
	}
	return nil
}

func (p *partition) reader(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	k, err := p.key(ctx, cacheType, remoteInstanceName, d)
	if err != nil {
		return nil, err
	}
	// Can't specify length because this might be ActionCache
	r, err := disk.FileReader(ctx, k.FullPath(), offset, limit)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.lru.Remove(k.FullPath()) // remove it just in case
		return nil, status.NotFoundErrorf("DiskCache missing file: %s", err)
	} else {
		p.lru.Get(k.FullPath()) // mark the file as used.
	}
	return r, nil
}

type dbCloseFn func(totalBytesWritten int64) error
type checkOversizeFn func(n int) error
type dbWriteOnClose struct {
	io.WriteCloser
	closeFn      dbCloseFn
	bytesWritten int64
}

func (d *dbWriteOnClose) Write(data []byte) (int, error) {
	n, err := d.WriteCloser.Write(data)
	d.bytesWritten += int64(n)
	return n, err
}

func (d *dbWriteOnClose) Close() error {
	if err := d.WriteCloser.Close(); err != nil {
		return err
	}
	return d.closeFn(d.bytesWritten)
}

func (p *partition) writer(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string, d *repb.Digest) (interfaces.CommittedWriteCloser, error) {
	k, err := p.key(ctx, cacheType, remoteInstanceName, d)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	alreadyExists := p.diskIsMapped && p.lru.Contains(k.FullPath())
	p.mu.Unlock()

	if alreadyExists {
		metrics.DiskCacheDuplicateWrites.Inc()
		metrics.DiskCacheDuplicateWritesBytes.Add(float64(d.GetSizeBytes()))
	}

	fw, err := disk.FileWriter(ctx, k.FullPath())
	if err != nil {
		return nil, err
	}
	cwc := ioutil.NewCustomCommitWriteCloser(fw)
	cwc.CommitFn = func(totalBytesWritten int64) error {
		record, err := makeRecordFromPath(k, k.FullPath())
		if err != nil {
			return err
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		p.lruAdd(record)
		metrics.DiskCacheAddedFileSizeBytes.Observe(float64(totalBytesWritten))
		return nil
	}
	return cwc, nil
}
