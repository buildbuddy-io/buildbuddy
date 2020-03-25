package disk_cache

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	cacheFullCutoffPercentage     = .98
	cacheEvictionCutoffPercentage = .8
)

// TODO(tylerw): Perf: Support inlining small blobs directly into the DB row
// TODO(tylerw): Perf: Support contains checks directly from the DB

// Blobstore is *almost* sufficient here, but some blobstore implementations
// don't support automatic TTL-ing of items, so we keep k/v style records in
// our DB that allow us to garbage collect expired blobs and keep an eye on
// the total cache size.
type DiskCache struct {
	bs interfaces.Blobstore
	db interfaces.Database

	ttl          time.Duration
	maxSizeBytes int64

	// ATOMIC INT
	totalSizeBytesAtomic int64

	ticker *time.Ticker
	quit   chan struct{}
}

// NB: ttl is a positive valued duration
func NewDiskCache(bs interfaces.Blobstore, db interfaces.Database, ttl time.Duration, maxSizeBytes int64) (*DiskCache, error) {
	if ttl < 0 {
		return nil, fmt.Errorf("ttl must be positive valued")
	}
	return &DiskCache{
		bs:           bs,
		db:           db,
		ttl:          ttl,
		maxSizeBytes: maxSizeBytes,
	}, nil
}

func (c *DiskCache) checkOverSize(n int) error {
	newSize := atomic.LoadInt64(&c.totalSizeBytesAtomic) + int64(n)
	if newSize > c.maxSizeBytes {
		errMsg := fmt.Sprintf("Cache size: %d bytes would exceed max: %d bytes", newSize, c.maxSizeBytes)
		return status.ResourceExhaustedError(errMsg)
	}
	return nil
}

func (c *DiskCache) Contains(ctx context.Context, key string) (bool, error) {
	return c.bs.BlobExists(ctx, key)
}

func (c *DiskCache) Get(ctx context.Context, key string) ([]byte, error) {
	err := c.db.IncrementEntryReadCount(ctx, key)
	if err != nil {
		return nil, err
	}
	return c.bs.ReadBlob(ctx, key)
}

func (c *DiskCache) Set(ctx context.Context, key string, data []byte) error {
	if err := c.checkOverSize(len(data)); err != nil {
		return err
	}

	n, err := c.bs.WriteBlob(ctx, key, data)
	if err != nil {
		return err
	}
	atomic.AddInt64(&c.totalSizeBytesAtomic, int64(n))
	return c.db.InsertOrUpdateCacheEntry(ctx, &tables.CacheEntry{
		EntryID:            key,
		ExpirationTimeUsec: int64(time.Now().Add(c.ttl).UnixNano() / 1000),
		SizeBytes:          int64(n),
	})
}

func (c *DiskCache) Delete(ctx context.Context, key string) error {
	if err := c.bs.DeleteBlob(ctx, key); err != nil {
		return err
	}
	return c.db.DeleteCacheEntry(ctx, key)
}

func (c *DiskCache) Reader(ctx context.Context, key string, offset, length int64) (io.Reader, error) {
	err := c.db.IncrementEntryReadCount(ctx, key)
	if err != nil {
		return nil, err
	}
	return c.bs.BlobReader(ctx, key, offset, length)
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
	blobWriter, err := c.bs.BlobWriter(ctx, key)
	if err != nil {
		return nil, err
	}
	return &dbWriteOnClose{
		WriteCloser: blobWriter,
		checkFn:     func(n int) error { return c.checkOverSize(n) },
		closeFn: func(totalBytesWritten int64) error {
			atomic.AddInt64(&c.totalSizeBytesAtomic, totalBytesWritten)
			return c.db.InsertOrUpdateCacheEntry(ctx, &tables.CacheEntry{
				EntryID:            key,
				ExpirationTimeUsec: int64(time.Now().Add(c.ttl).UnixNano() / 1000),
				SizeBytes:          totalBytesWritten,
			})
		},
	}, nil
}

func (c *DiskCache) deleteCacheEntries(ctx context.Context, entries []*tables.CacheEntry) {
	for _, entry := range entries {
		err := c.db.DeleteCacheEntry(ctx, entry.EntryID)
		if err != nil {
			log.Printf("error deleting expired cache entry: %s: %s", entry, err)
		}
	}
}

func (c *DiskCache) evictExpiredEntries(ctx context.Context) {
	cutoffUsec := time.Now().Add(-1*c.ttl).UnixNano() / 1000
	expiredEntries, err := c.db.RawQueryCacheEntries(ctx,
		`SELECT * FROM CacheEntries as ce WHERE ce.created_at_usec < ?`,
		cutoffUsec)
	if err != nil {
		log.Printf("error querying expired cache entries: %s", err)
	}
	c.deleteCacheEntries(ctx, expiredEntries)
}

func (c *DiskCache) recalculateSizeAndExpireEntries(ctx context.Context) {
	sizeFn := func() int64 {
		sum, err := c.db.SumCacheEntrySizes(ctx)
		if err != nil {
			log.Printf("Error calculating cache size: %s", err)
		}
		atomic.StoreInt64(&c.totalSizeBytesAtomic, sum)
		return sum
	}

	currentSize := sizeFn()

	// Only trigger if the cache is ~full~.
	fullCutoff := int64(cacheFullCutoffPercentage * float64(c.maxSizeBytes))
	if currentSize < fullCutoff {
		return
	}

	var fullWarningMsg string
	fullWarningMsg += fmt.Sprintf("Cache is ~full~ (%d bytes of %d)\n", currentSize, c.maxSizeBytes)
	fullWarningMsg += fmt.Sprintf("Deleting least used items until we reach %f percent capacity...\n", (cacheEvictionCutoffPercentage * 100))
	fullWarningMsg += "You may want to consider allocating more space or reducing your TTL\n"
	fullWarningMsg += "(Or not! It's perfectly OK to let this one ride if you know what's going on)"

	log.Printf(fullWarningMsg)

	stopDeleteCutoff := int64(cacheEvictionCutoffPercentage * float64(c.maxSizeBytes))
	// Delete entries until we reach 90% capacity.
	for {
		leastUsedEntries, err := c.db.RawQueryCacheEntries(ctx,
			`SELECT * FROM CacheEntries as ce ORDER BY read_count ASC LIMIT 10`)
		if err != nil {
			log.Printf("error querying oldest cache entries: %s", err)
		}
		c.deleteCacheEntries(ctx, leastUsedEntries)
		currentSize = sizeFn()
		if currentSize < stopDeleteCutoff {
			log.Printf("Deleted least used items from cache. (%d bytes of %d)", currentSize, c.maxSizeBytes)
			return
		}
	}
}

func (c *DiskCache) Start() error {
	c.ticker = time.NewTicker(1 * time.Second)
	c.quit = make(chan struct{})

	if c.ttl == 0 {
		log.Printf("configured TTL was 0; disabling cache expiry")
		return nil
	}

	go func() {
		for {
			select {
			case <-c.ticker.C:
				ctx := context.Background()
				c.recalculateSizeAndExpireEntries(ctx)
				c.evictExpiredEntries(ctx)
			case <-c.quit:
				log.Printf("Cleanup task %d exiting.", 0)
				break
			}
		}
	}()

	return nil
}

func (c *DiskCache) Stop() error {
	close(c.quit)
	c.ticker.Stop()
	return nil
}
