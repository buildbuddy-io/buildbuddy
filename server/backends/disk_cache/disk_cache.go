package disk_cache

import (
	"context"
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

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// cutoffThreshold is the point above which a janitor thread will run
	// and delete the oldest items from the cache.
	janitorCutoffThreshold = .90

	// janitorCheckPeriod is how often the janitor thread will wake up to
	// check the cache size.
	janitorCheckPeriod = 100 * time.Millisecond

	defaultPartitionID = "default"
)

// DiskCache stores data on disk as files.
// It is broken up into partitions which are independent and maintain their own LRUs.
type DiskCache struct {
	env               environment.Env
	partitions        map[string]*partition
	partitionMappings []config.DiskCachePartitionMapping
	// The currently selected partition. Initialized to the default partition.
	// WithRemoteInstanceName can create a new cache accessor with a different selected partition.
	partition *partition
	prefix    string
}

func NewDiskCache(env environment.Env, config *config.DiskConfig, defaultMaxSizeBytes int64) (*DiskCache, error) {
	partitions := make(map[string]*partition)
	var defaultPartition *partition
	for _, pc := range config.Partitions {
		rootDir := config.RootDirectory
		if pc.ID != defaultPartitionID {
			if pc.ID == "" {
				return nil, status.InvalidArgumentError("Non-default partition %q must have a valid ID")
			}
			rootDir = filepath.Join(rootDir, pc.ID)
		}

		p, err := newPartition(pc.ID, rootDir, pc.MaxSizeBytes)
		if err != nil {
			return nil, err
		}
		partitions[pc.ID] = p
		if pc.ID == defaultPartitionID {
			defaultPartition = p
		}
	}
	if defaultPartition == nil {
		p, err := newPartition(defaultPartitionID, config.RootDirectory, defaultMaxSizeBytes)
		if err != nil {
			return nil, err
		}
		defaultPartition = p
		partitions[defaultPartitionID] = p
	}

	c := &DiskCache{
		env:               env,
		partitions:        partitions,
		partitionMappings: config.PartitionMappings,
		partition:         defaultPartition,
	}
	statusz.AddSection("disk_cache", "On disk LRU cache", c)
	return c, nil
}

func (c *DiskCache) WithPrefix(prefix string) interfaces.Cache {
	newPrefix := filepath.Join(append(filepath.SplitList(c.prefix), prefix)...)
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}

	return &DiskCache{
		env:               c.env,
		prefix:            newPrefix,
		partition:         c.partition,
		partitions:        c.partitions,
		partitionMappings: c.partitionMappings,
	}
}

func (c *DiskCache) WithRemoteInstanceName(ctx context.Context, remoteInstanceName string) (interfaces.Cache, error) {
	auth := c.env.GetAuthenticator()
	if auth == nil {
		return c, nil
	}
	user, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return c, nil
	}
	for _, m := range c.partitionMappings {
		if m.GroupID == user.GetGroupID() && strings.HasPrefix(remoteInstanceName, m.Prefix) {
			p, ok := c.partitions[m.PartitionID]
			if !ok {
				return nil, status.NotFoundErrorf("Mapping to unknown partition %q", m.PartitionID)
			}
			return &DiskCache{
				env:               c.env,
				prefix:            c.prefix,
				partition:         p,
				partitions:        c.partitions,
				partitionMappings: c.partitionMappings,
			}, nil
		}
	}
	return c, nil
}

func (c *DiskCache) Statusz(ctx context.Context) string {
	buf := ""
	for _, p := range c.partitions {
		buf += p.Statusz(ctx)
	}
	return buf
}

func (c *DiskCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	return c.partition.contains(ctx, c.prefix, d)
}

func (c *DiskCache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	return c.partition.containsMulti(ctx, c.prefix, digests)
}

func (c *DiskCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	return c.partition.get(ctx, c.prefix, d)
}

func (c *DiskCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	return c.partition.getMulti(ctx, c.prefix, digests)
}

func (c *DiskCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	return c.partition.set(ctx, c.prefix, d, data)
}

func (c *DiskCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	return c.partition.setMulti(ctx, c.prefix, kvs)
}

func (c *DiskCache) Delete(ctx context.Context, d *repb.Digest) error {
	return c.partition.delete(ctx, c.prefix, d)
}

func (c *DiskCache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	return c.partition.reader(ctx, c.prefix, d, offset)
}

func (c *DiskCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	return c.partition.writer(ctx, c.prefix, d)
}

// We keep a record (in memory) of file atime (Last Access Time) and size, and
// when our cache reaches maxSize we remove the oldest files. Rather than
// serialize this ledger, we regenerate it from scratch on startup by looking
// at the filesystem.
type partition struct {
	id           string
	mu           sync.RWMutex
	rootDir      string
	lru          *lru.LRU
	fileChannel  chan *fileRecord
	diskIsMapped bool
	lastGCTime   time.Time
}

func newPartition(id string, rootDir string, maxSizeBytes int64) (*partition, error) {
	l, err := lru.NewLRU(&lru.Config{MaxSize: maxSizeBytes, OnEvict: evictFn, SizeFn: sizeFn})
	if err != nil {
		return nil, err
	}
	c := &partition{
		id:          id,
		lru:         l,
		rootDir:     rootDir,
		fileChannel: make(chan *fileRecord),
	}
	if err := c.initializeCache(); err != nil {
		return nil, err
	}
	c.startJanitor()
	return c, nil
}

type fileRecord struct {
	key       string
	lastUse   int64
	sizeBytes int64
}

func sizeFn(key interface{}, value interface{}) int64 {
	size := int64(0)
	if v, ok := value.(*fileRecord); ok {
		size += v.sizeBytes
	}
	return size
}

func evictFn(key interface{}, value interface{}) {
	if k, ok := key.(string); ok {
		disk.DeleteFile(context.TODO(), k)
	}
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
	return time.Unix(ts.Sec, ts.Nsec).UnixNano()
}

func makeRecord(fullPath string, info os.FileInfo) *fileRecord {
	return &fileRecord{
		key:       fullPath,
		sizeBytes: info.Size(),
		lastUse:   getLastUse(info),
	}
}

func (p *partition) Statusz(ctx context.Context) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	buf := "<br>"
	buf += fmt.Sprintf("<div>Partition %q</div>", p.id)
	buf += fmt.Sprintf("<div>Root directory: %s</div>", p.rootDir)
	percentFull := float64(p.lru.Size()) / float64(p.lru.MaxSize()) * 100.0
	buf += fmt.Sprintf("<div>Capacity: %d / %d (%2.2f%% full)</div>", p.lru.Size(), p.lru.MaxSize(), percentFull)
	var oldestItem time.Time
	if _, v, ok := p.lru.GetOldest(); ok {
		if fr, ok := v.(*fileRecord); ok {
			oldestItem = time.Unix(0, fr.lastUse)
		}
	}
	buf += fmt.Sprintf("<div>%d items (oldest: %s)</div>", p.lru.Len(), oldestItem.Format("Jan 02, 2006 15:04:05 PST"))
	buf += fmt.Sprintf("<div>Mapped into LRU: %t</div>", p.diskIsMapped)
	buf += fmt.Sprintf("<div>GC Last run: %s</div>", p.lastGCTime.Format("Jan 02, 2006 15:04:05 PST"))
	return buf
}

func (p *partition) reduceCacheSize(targetSize int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lru.Size() < targetSize {
		return false
	}
	_, value, ok := p.lru.RemoveOldest()
	if !ok {
		return false // should never happen
	}
	if f, ok := value.(*fileRecord); ok {
		log.Debugf("Delete thread removed item from cache. Last use was: %d", f.lastUse)
	}
	p.lastGCTime = time.Now()
	return true
}

func (p *partition) startJanitor() {
	targetSize := int64(float64(p.lru.MaxSize()) * janitorCutoffThreshold)
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
		records := make([]*fileRecord, 0)
		inFlightRecords := make([]*fileRecord, 0)
		go func() {
			for record := range p.fileChannel {
				inFlightRecords = append(inFlightRecords, record)
			}
		}()
		walkFn := func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				info, err := d.Info()
				if err != nil {
					return err
				}
				if info.Size() == 0 {
					log.Debugf("Skipping 0 length file: %q", path)
					return nil
				}
				records = append(records, makeRecord(path, info))
			}
			return nil
		}
		if err := filepath.WalkDir(p.rootDir, walkFn); err != nil {
			log.Warningf("Error walking disk directory: %s", err)
		}

		// Sort entries by ascending ATime.
		sort.Slice(records, func(i, j int) bool { return records[i].lastUse < records[j].lastUse })

		p.mu.Lock()
		// Populate our LRU with everything we scanned from disk.
		for _, record := range records {
			p.lru.Add(record.key, record)
		}
		// Add in-flight records to the LRU. These were new files
		// touched during the loading phase, so we assume they are new
		// enough to just add to the top of the LRU without sorting.
		close(p.fileChannel)
		for _, record := range inFlightRecords {
			p.lru.Add(record.key, record)
		}
		p.diskIsMapped = true
		p.mu.Unlock()

		log.Debugf("DiskCache partition %q: statd %d files in %s", p.id, len(records), time.Since(start))
		log.Infof("Finished initializing disk cache partition %q at %q. Current size: %d (max: %d) bytes", p.id, p.rootDir, p.lru.Size(), p.lru.MaxSize())
	}()
	return nil
}

func (p *partition) key(ctx context.Context, cachePrefix string, d *repb.Digest) (string, error) {
	hash, err := digest.Validate(d)
	if err != nil {
		return "", err
	}
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", err
	}

	return filepath.Join(p.rootDir, userPrefix+cachePrefix+hash), nil
}

// Adds a single file, using the provided path, to the LRU.
// NB: Callers are responsible for locking the LRU before calling this function.
func (p *partition) addFileToLRUIfExists(k string) bool {
	if p.diskIsMapped {
		return false
	}
	info, err := os.Stat(k)
	if err == nil {
		if info.Size() == 0 {
			log.Debugf("Skipping 0 length file: %q", k)
			return false
		}
		record := makeRecord(k, info)
		p.fileChannel <- record
		p.lru.Add(record.key, record)
		return true
	}
	return false
}

func (p *partition) contains(ctx context.Context, prefix string, d *repb.Digest) (bool, error) {
	k, err := p.key(ctx, prefix, d)
	if err != nil {
		return false, err
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
	_, ok := p.lru.Get(k)

	if !ok && !p.diskIsMapped {
		// OK if we're here it means the disk contents are still being loaded
		// into the LRU. But we still need to return an answer! So we'll go
		// check the FS, and if the file is there we'll add it to the LRU.
		ok = p.addFileToLRUIfExists(k)
	}
	return ok, nil
}

func (p *partition) containsMulti(ctx context.Context, prefix string, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest]bool, len(digests))
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				exists, err := p.contains(ctx, prefix, d)
				// NotFoundError is never returned from contains above, so
				// we don't check for it.
				if err != nil {
					return err
				}
				lock.Lock()
				foundMap[d] = exists
				lock.Unlock()
				return nil
			})
		}
		fetchFn(d)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return foundMap, nil
}

func (p *partition) get(ctx context.Context, prefix string, d *repb.Digest) ([]byte, error) {
	k, err := p.key(ctx, prefix, d)
	if err != nil {
		return nil, err
	}
	buf, err := disk.ReadFile(ctx, k)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.lru.Remove(k) // remove it just in case
		return nil, status.NotFoundErrorf("DiskCache missing file: %s", err)
	}

	if p.lru.Contains(k) {
		p.lru.Get(k) // mark the file as used.
	} else if !p.diskIsMapped {
		p.addFileToLRUIfExists(k)
	}
	return buf, nil
}

func (p *partition) getMulti(ctx context.Context, prefix string, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest][]byte, len(digests))
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				data, err := p.get(ctx, prefix, d)
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
		fetchFn(d)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return foundMap, nil
}

func (p *partition) set(ctx context.Context, prefix string, d *repb.Digest, data []byte) error {
	k, err := p.key(ctx, prefix, d)
	if err != nil {
		return err
	}
	n, err := disk.WriteFile(ctx, k, data)
	if err != nil {
		// If we had an error writing the file, just return that.
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lru.Add(k, &fileRecord{
		key:       k,
		sizeBytes: int64(n),
		lastUse:   time.Now().UnixNano(),
	})
	return err

}

func (p *partition) setMulti(ctx context.Context, prefix string, kvs map[*repb.Digest][]byte) error {
	for d, data := range kvs {
		if err := p.set(ctx, prefix, d, data); err != nil {
			return err
		}
	}
	return nil
}

func (p *partition) delete(ctx context.Context, prefix string, d *repb.Digest) error {
	k, err := p.key(ctx, prefix, d)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lru.Remove(k)
	return nil
}

func (p *partition) reader(ctx context.Context, prefix string, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	k, err := p.key(ctx, prefix, d)
	if err != nil {
		return nil, err
	}
	// Can't specify length because this might be ActionCache
	r, err := disk.FileReader(ctx, k, offset, 0)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.lru.Remove(k) // remove it just in case
		return nil, status.NotFoundErrorf("DiskCache missing file: %s", err)
	} else {
		p.lru.Get(k) // mark the file as used.
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

func (p *partition) writer(ctx context.Context, prefix string, d *repb.Digest) (io.WriteCloser, error) {
	k, err := p.key(ctx, prefix, d)
	if err != nil {
		return nil, err
	}

	writeCloser, err := disk.FileWriter(ctx, k)
	if err != nil {
		return nil, err
	}
	return &dbWriteOnClose{
		WriteCloser: writeCloser,
		closeFn: func(totalBytesWritten int64) error {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.lru.Add(k, &fileRecord{
				key:       k,
				sizeBytes: totalBytesWritten,
				lastUse:   time.Now().UnixNano(),
			})
			return nil
		},
	}, nil
}
