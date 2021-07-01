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
	"sync"
	"syscall"
	"time"

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
)

// We keep a record (in memory) of file atime (Last Access Time) and size, and
// when our cache reaches maxSize we remove the oldest files. Rather than
// serialize this ledger, we regenerate it from scratch on startup by looking
// at the filesystem.
type DiskCache struct {
	l            *lru.LRU
	mu           *sync.RWMutex
	fileChannel  chan *fileRecord
	rootDir      string
	prefix       string
	diskIsMapped *bool
	lastGCTime   time.Time
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

func NewDiskCache(rootDir string, maxSizeBytes int64) (*DiskCache, error) {
	l, err := lru.NewLRU(&lru.Config{MaxSize: maxSizeBytes, OnEvict: evictFn, SizeFn: sizeFn})
	if err != nil {
		return nil, err
	}
	notMappedBool := false
	c := &DiskCache{
		l:            l,
		rootDir:      rootDir,
		mu:           &sync.RWMutex{},
		fileChannel:  make(chan *fileRecord),
		diskIsMapped: &notMappedBool,
	}
	if err := c.initializeCache(); err != nil {
		return nil, err
	}
	c.startJanitor()
	statusz.AddSection("disk_cache", "On disk LRU cache", c)
	return c, nil
}

func (c *DiskCache) Statusz(ctx context.Context) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	buf := ""
	buf += fmt.Sprintf("<div>Root directory: %s</div>", c.rootDir)
	percentFull := float64(c.l.Size()) / float64(c.l.MaxSize()) * 100.0
	buf += fmt.Sprintf("<div>Capacity: %d / %d (%2.2f%% full)</div>", c.l.Size(), c.l.MaxSize(), percentFull)
	var oldestItem time.Time
	if _, v, ok := c.l.GetOldest(); ok {
		if fr, ok := v.(*fileRecord); ok {
			oldestItem = time.Unix(0, fr.lastUse)
		}
	}
	buf += fmt.Sprintf("<div>%d items (oldest: %s)</div>", c.l.Len(), oldestItem.Format("Jan 02, 2006 15:04:05 PST"))
	buf += fmt.Sprintf("<div>Mapped into LRU: %t</div>", *c.diskIsMapped)
	buf += fmt.Sprintf("<div>GC Last run: %s</div>", c.lastGCTime.Format("Jan 02, 2006 15:04:05 PST"))
	return buf
}

func (c *DiskCache) WithPrefix(prefix string) interfaces.Cache {
	c.mu.Lock()
	defer c.mu.Unlock()

	newPrefix := filepath.Join(append(filepath.SplitList(c.prefix), prefix)...)
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}

	return &DiskCache{
		l:            c.l,
		rootDir:      c.rootDir,
		mu:           c.mu,
		prefix:       newPrefix,
		fileChannel:  c.fileChannel,
		diskIsMapped: c.diskIsMapped,
		lastGCTime:   c.lastGCTime,
	}
}

func (c *DiskCache) reduceCacheSize(targetSize int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.l.Size() < targetSize {
		return false
	}
	_, value, ok := c.l.RemoveOldest()
	if !ok {
		return false // should never happen
	}
	if f, ok := value.(*fileRecord); ok {
		log.Debugf("Delete thread removed item from cache. Last use was: %d", f.lastUse)
	}
	c.lastGCTime = time.Now()
	return true
}

func (c *DiskCache) startJanitor() {
	targetSize := int64(float64(c.l.MaxSize()) * janitorCutoffThreshold)
	go func() {
		for {
			select {
			case <-time.After(janitorCheckPeriod):
				for {
					if !c.reduceCacheSize(targetSize) {
						break
					}
				}
			}
		}
	}()
}

func (c *DiskCache) initializeCache() error {
	if err := disk.EnsureDirectoryExists(c.rootDir); err != nil {
		return err
	}

	go func() {
		start := time.Now()
		records := make([]*fileRecord, 0)
		inFlightRecords := make([]*fileRecord, 0)
		go func() {
			for record := range c.fileChannel {
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
		if err := filepath.WalkDir(c.rootDir, walkFn); err != nil {
			log.Warningf("Error walking disk directory: %s", err)
		}

		// Sort entries by ascending ATime.
		sort.Slice(records, func(i, j int) bool { return records[i].lastUse < records[j].lastUse })

		c.mu.Lock()
		// Populate our LRU with everything we scanned from disk.
		for _, record := range records {
			c.l.Add(record.key, record)
		}
		// Add in-flight records to the LRU. These were new files
		// touched during the loading phase, so we assume they are new
		// enough to just add to the top of the LRU without sorting.
		close(c.fileChannel)
		for _, record := range inFlightRecords {
			c.l.Add(record.key, record)
		}
		*c.diskIsMapped = true
		c.mu.Unlock()

		log.Debugf("DiskCache: statd %d files in %s", len(records), time.Since(start))
		log.Infof("Finished initializing disk cache at %q. Current size: %d (max: %d) bytes", c.rootDir, c.l.Size(), c.l.MaxSize())
	}()
	return nil
}

func (c *DiskCache) key(ctx context.Context, d *repb.Digest) (string, error) {
	hash, err := digest.Validate(d)
	if err != nil {
		return "", err
	}
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", err
	}

	return filepath.Join(c.rootDir, userPrefix+c.prefix+hash), nil
}

// Adds a single file, using the provided path, to the LRU.
// NB: Callers are responsible for locking the LRU before calling this function.
func (c *DiskCache) addFileToLRUIfExists(k string) bool {
	if *c.diskIsMapped {
		return false
	}
	info, err := os.Stat(k)
	if err == nil {
		if info.Size() == 0 {
			log.Debugf("Skipping 0 length file: %q", k)
			return false
		}
		record := makeRecord(k, info)
		c.fileChannel <- record
		c.l.Add(record.key, record)
		return true
	}
	return false
}

func (c *DiskCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	k, err := c.key(ctx, d)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.l.Get(k)

	if !ok && !*c.diskIsMapped {
		// OK if we're here it means the disk contents are still being loaded
		// into the LRU. But we still need to return an answer! So we'll go
		// check the FS, and if the file is there we'll add it to the LRU.
		ok = c.addFileToLRUIfExists(k)
	}
	return ok, nil
}

func (c *DiskCache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest]bool, len(digests))
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				exists, err := c.Contains(ctx, d)
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

func (c *DiskCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	k, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	buf, err := disk.ReadFile(ctx, k)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.l.Remove(k) // remove it just in case
		return nil, status.NotFoundErrorf("DiskCache missing file: %s", err)
	}

	if c.l.Contains(k) {
		c.l.Get(k) // mark the file as used.
	} else if !*c.diskIsMapped {
		c.addFileToLRUIfExists(k)
	}
	return buf, nil
}

func (c *DiskCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest][]byte, len(digests))
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				data, err := c.Get(ctx, d)
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

func (c *DiskCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	k, err := c.key(ctx, d)
	if err != nil {
		return err
	}
	n, err := disk.WriteFile(ctx, k, data)
	if err != nil {
		// If we had an error writing the file, just return that.
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.l.Add(k, &fileRecord{
		key:       k,
		sizeBytes: int64(n),
		lastUse:   time.Now().UnixNano(),
	})
	return err

}

func (c *DiskCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	for d, data := range kvs {
		if err := c.Set(ctx, d, data); err != nil {
			return err
		}
	}
	return nil
}

func (c *DiskCache) Delete(ctx context.Context, d *repb.Digest) error {
	k, err := c.key(ctx, d)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.l.Remove(k)
	return nil
}

func (c *DiskCache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	k, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	// Can't specify length because this might be ActionCache
	r, err := disk.FileReader(ctx, k, offset, 0)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.l.Remove(k) // remove it just in case
		return nil, status.NotFoundErrorf("DiskCache missing file: %s", err)
	} else {
		c.l.Get(k) // mark the file as used.
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

func (c *DiskCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	k, err := c.key(ctx, d)
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
			c.mu.Lock()
			defer c.mu.Unlock()
			c.l.Add(k, &fileRecord{
				key:       k,
				sizeBytes: totalBytesWritten,
				lastUse:   time.Now().UnixNano(),
			})
			return nil
		},
	}, nil
}

func (c *DiskCache) Start() error {
	return nil
}

func (c *DiskCache) Stop() error {
	return nil
}
