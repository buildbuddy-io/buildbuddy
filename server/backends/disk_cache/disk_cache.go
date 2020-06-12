package disk_cache

import (
	"container/list"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	cacheEvictionCutoffPercentage = .8
)

// Blobstore is *almost* sufficient here, but blobstore doesn't handle record
// expirations in a way that make sense for a cache. So we keep a record (in
// memory) of file atime (Last Access Time) and size, and then when our cache
// reaches fullness we remove the most stale files. Rather than serialize this
// ledger, we regenerate it from scratch on startup by looking at the
// filesystem itself.
type DiskCache struct {
	rootDir      string
	maxSizeBytes int64

	lock      sync.RWMutex // PROTECTS: evictList, entries, sizeBytes
	evictList *list.List
	entries   map[string]*list.Element
	sizeBytes int64
}

type fileRecord struct {
	key       string
	sizeBytes int64
	lastUse   time.Time
}

func getLastUse(info os.FileInfo) time.Time {
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
	return time.Unix(ts.Sec, ts.Nsec)
}

func makeRecord(fullPath string, info os.FileInfo) *fileRecord {
	return &fileRecord{
		key:       fullPath,
		sizeBytes: info.Size(),
		lastUse:   getLastUse(info),
	}
}

func NewDiskCache(rootDir string, maxSizeBytes int64) (*DiskCache, error) {
	c := &DiskCache{
		rootDir:      rootDir,
		maxSizeBytes: maxSizeBytes,
	}
	if err := c.initializeCache(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *DiskCache) initializeCache() error {
	if err := disk.EnsureDirectoryExists(c.rootDir); err != nil {
		return err
	}
	records := make([]*fileRecord, 0)
	walkFn := func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			records = append(records, makeRecord(path, info))
		}
		return nil
	}
	if err := filepath.Walk(c.rootDir, walkFn); err != nil {
		return err
	}

	// Sort entries by ascending ATime.
	sort.Slice(records, func(i, j int) bool { return records[i].lastUse.Before(records[j].lastUse) })

	// Populate our state tracking datastructures.
	c.evictList = list.New()
	c.entries = make(map[string]*list.Element, len(records))
	for _, record := range records {
		c.addEntry(record)
		c.sizeBytes += record.sizeBytes
	}
	log.Printf("Initialized disk cache. Current size: %d (max: %d) bytes", c.sizeBytes, c.maxSizeBytes)
	return nil
}

func (c *DiskCache) addEntry(record *fileRecord) {
	c.lock.Lock()
	listElement := c.evictList.PushFront(record)
	c.entries[record.key] = listElement
	c.lock.Unlock()
}

func (c *DiskCache) removeEntry(key string) {
	c.lock.Lock()
	if listElement, ok := c.entries[key]; ok {
		c.evictList.Remove(listElement)
		record := listElement.Value.(*fileRecord)
		c.sizeBytes -= record.sizeBytes
	}
	c.lock.Unlock()
}

func (c *DiskCache) useEntry(key string) bool {
	c.lock.Lock()
	listElement, ok := c.entries[key]
	if ok {
		c.evictList.MoveToFront(listElement)
		listElement.Value.(*fileRecord).lastUse = time.Now()
	}
	c.lock.Unlock()
	return ok
}

func (c *DiskCache) checkSizeAndEvict(ctx context.Context, n int64) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	newSize := c.sizeBytes + n
	stopDeleteCutoff := int64(cacheEvictionCutoffPercentage * float64(c.maxSizeBytes))

	if newSize > c.maxSizeBytes {
		for c.sizeBytes+n > stopDeleteCutoff {
			listElement := c.evictList.Back()
			if listElement != nil {
				c.evictList.Remove(listElement)
				record := listElement.Value.(*fileRecord)
				c.sizeBytes -= record.sizeBytes
				if err := disk.DeleteFile(ctx, record.key); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (c *DiskCache) PrefixKey(ctx context.Context, blobName string) (string, error) {
	// Probably could be more careful here but we are generating these ourselves
	// for now.
	if strings.Contains(blobName, "..") {
		return "", status.InvalidArgumentErrorf("blobName (%s) must not contain ../", blobName)
	}
	return filepath.Join(c.rootDir, blobName), nil
}

func (c *DiskCache) Contains(ctx context.Context, key string) (bool, error) {
	fullPath, err := c.PrefixKey(ctx, key)
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
	return c.useEntry(fullPath), nil
}

func (c *DiskCache) ContainsMulti(ctx context.Context, keys []string) (map[string]bool, error) {
	foundMap := make(map[string]bool, len(keys))
	// No parallelism here -- we don't know enough about what kind of io
	// characteristics our disk has anyway. And disk is usually used for
	// on-prem / small instances where this doesn't matter as much.
	for _, key := range keys {
		ok, err := c.Contains(ctx, key)
		if err != nil {
			return nil, err
		}
		foundMap[key] = ok
	}
	return foundMap, nil
}

func (c *DiskCache) Get(ctx context.Context, key string) ([]byte, error) {
	fullPath, err := c.PrefixKey(ctx, key)
	if err != nil {
		return nil, err
	}
	c.useEntry(fullPath)
	return disk.ReadFile(ctx, fullPath)
}

func (c *DiskCache) GetMulti(ctx context.Context, keys []string) (map[string][]byte, error) {
	foundMap := make(map[string][]byte, len(keys))
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, key := range keys {
		data, err := c.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		foundMap[key] = data
	}
	return foundMap, nil
}

func (c *DiskCache) Set(ctx context.Context, key string, data []byte) error {
	fullPath, err := c.PrefixKey(ctx, key)
	if err != nil {
		return err
	}
	if err := c.checkSizeAndEvict(ctx, int64(len(data))); err != nil {
		return err
	}
	n, err := disk.WriteFile(ctx, fullPath, data)
	if err != nil {
		// If we had an error writing the file, just return that.
		return err
	}
	c.addEntry(&fileRecord{
		key:       fullPath,
		sizeBytes: int64(n),
		lastUse:   time.Now(),
	})
	return err

}

func (c *DiskCache) Delete(ctx context.Context, key string) error {
	fullPath, err := c.PrefixKey(ctx, key)
	if err != nil {
		return err
	}
	c.removeEntry(fullPath)
	return disk.DeleteFile(ctx, fullPath)
}

func (c *DiskCache) Reader(ctx context.Context, key string, offset, length int64) (io.Reader, error) {
	fullPath, err := c.PrefixKey(ctx, key)
	if err != nil {
		return nil, err
	}
	c.useEntry(fullPath)
	return disk.FileReader(ctx, fullPath, offset, length)
}

type dbCloseFn func(totalBytesWritten int64) error
type checkOversizeFn func(n int) error
type dbWriteOnClose struct {
	io.WriteCloser
	checkFn      checkOversizeFn
	closeFn      dbCloseFn
	bytesWritten int64
}

func (d *dbWriteOnClose) Write(data []byte) (int, error) {
	if err := d.checkFn(len(data)); err != nil {
		return 0, err
	}
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

func (c *DiskCache) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	fullPath, err := c.PrefixKey(ctx, key)
	if err != nil {
		return nil, err
	}
	writeCloser, err := disk.FileWriter(ctx, fullPath)
	if err != nil {
		return nil, err
	}
	return &dbWriteOnClose{
		WriteCloser: writeCloser,
		checkFn:     func(n int) error { return c.checkSizeAndEvict(ctx, int64(n)) },
		closeFn: func(totalBytesWritten int64) error {
			if err := c.checkSizeAndEvict(ctx, totalBytesWritten); err != nil {
				return err
			}
			c.addEntry(&fileRecord{
				key:       fullPath,
				sizeBytes: totalBytesWritten,
				lastUse:   time.Now(),
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
