package disk_cache

import (
	"context"
	"io"
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
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// We keep a record (in memory) of file atime (Last Access Time) and size, and
// when our cache reaches maxSize we remove the oldest files. Rather than
// serialize this ledger, we regenerate it from scratch on startup by looking
// at the filesystem.
type DiskCache struct {
	rootDir string
	prefix  string
	l       *lru.LRU
	lock    *sync.RWMutex
}

type fileRecord struct {
	key       string
	sizeBytes int64
	lastUse   time.Time
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
	l, err := lru.NewLRU(&lru.Config{MaxSize: maxSizeBytes, OnEvict: evictFn, SizeFn: sizeFn})
	if err != nil {
		return nil, err
	}
	c := &DiskCache{
		l:       l,
		rootDir: rootDir,
		lock:    &sync.RWMutex{},
	}
	if err := c.initializeCache(); err != nil {
		return nil, err
	}
	log.Infof("Initialized disk cache at %q. Current size: %d (max: %d) bytes", c.rootDir, c.l.Size(), c.l.MaxSize())
	return c, nil
}

func (c *DiskCache) WithPrefix(prefix string) interfaces.Cache {
	newPrefix := filepath.Join(append(filepath.SplitList(c.prefix), prefix)...)
	if len(newPrefix) > 0 && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}

	return &DiskCache{
		l:       c.l,
		rootDir: c.rootDir,
		lock:    c.lock,
		prefix:  newPrefix,
	}
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
	for _, record := range records {
		c.l.Add(record.key, record)
	}
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
	c.lock.Lock()
	_, ok := c.l.Get(k)
	c.lock.Unlock()
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
				if err != nil {
					return err
				}
				lock.Lock()
				defer lock.Unlock()
				foundMap[d] = exists
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
	f, err := disk.ReadFile(ctx, k)
	c.lock.Lock()
	defer c.lock.Unlock()
	if err != nil {
		c.l.Remove(k) // remove it just in case
		return nil, status.NotFoundErrorf("DiskCache missing file: %s", err)
	} else {
		c.l.Get(k) // mark the file as used.
	}
	return f, nil
}

func (c *DiskCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	lock := sync.RWMutex{} // protects(foundMap)
	foundMap := make(map[*repb.Digest][]byte, len(digests))
	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range digests {
		fetchFn := func(d *repb.Digest) {
			eg.Go(func() error {
				data, err := c.Get(ctx, d)
				if err != nil {
					return err
				}
				lock.Lock()
				defer lock.Unlock()
				foundMap[d] = data
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
	c.lock.Lock()
	defer c.lock.Unlock()
	c.l.Add(k, &fileRecord{
		key:       k,
		sizeBytes: int64(n),
		lastUse:   time.Now(),
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
	c.lock.Lock()
	defer c.lock.Unlock()
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
	c.lock.Lock()
	defer c.lock.Unlock()
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
			c.lock.Lock()
			defer c.lock.Unlock()
			c.l.Add(k, &fileRecord{
				key:       k,
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
