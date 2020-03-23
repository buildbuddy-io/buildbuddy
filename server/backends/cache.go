package cache

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
)

// Blobstore is *almost* sufficient here, but some blobstore implementations
// don't support automatic TTL-ing of items, so we keep k/v style records in
// our DB that allow us to garbage collect expired blobs and keep an eye on
// the total cache size.
type Cache struct {
	bs interfaces.Blobstore
	db interfaces.Database

	ttl          time.Duration
	maxSizeBytes int64
}

// NB: ttl is a positive valued duration
func NewCache(bs interfaces.Blobstore, db interfaces.Database, ttl time.Duration, maxSizeBytes int64) (*Cache, error) {
	if ttl < 0 {
		return nil, fmt.Errorf("ttl must be positive valued")
	}
	return &Cache{
		bs:           bs,
		db:           db,
		ttl:          ttl,
		maxSizeBytes: maxSizeBytes,
	}, nil
}

func (c *Cache) Contains(ctx context.Context, key string) (bool, error) {
	return c.bs.BlobExists(ctx, key)
}

func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	return c.bs.ReadBlob(ctx, key)
}

func (c *Cache) Set(ctx context.Context, key string, data []byte) error {
	if err := c.bs.WriteBlob(ctx, key, data); err != nil {
		return err
	}
	return c.db.InsertOrUpdateCacheEntry(ctx, &tables.CacheEntry{
		EntryID:            key,
		ExpirationTimeUsec: int64(time.Now().Add(c.ttl).UnixNano() / 1000),
	})
}

func (c *Cache) Delete(ctx context.Context, key string) error {
	if err := c.bs.DeleteBlob(ctx, key); err != nil {
		return err
	}
	return c.db.DeleteCacheEntry(ctx, key)
}

func (c *Cache) Reader(ctx context.Context, key string, offset, length int64) (io.Reader, error) {
	return c.bs.BlobReader(ctx, key, offset, length)
}

type dbCloseFn func() error
type dbWriteOnClose struct {
	io.WriteCloser
	closeFn dbCloseFn
}

func (d *dbWriteOnClose) Close() error {
	if err := d.WriteCloser.Close(); err != nil {
		return err
	}
	return d.closeFn()
}

func (c *Cache) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	blobWriter, err := c.bs.BlobWriter(ctx, key)
	if err != nil {
		return nil, err
	}
	return &dbWriteOnClose{
		WriteCloser: blobWriter,
		closeFn: func() error {
			return c.db.InsertOrUpdateCacheEntry(ctx, &tables.CacheEntry{
				EntryID:            key,
				ExpirationTimeUsec: int64(time.Now().Add(c.ttl).UnixNano() / 1000),
			})
		},
	}, nil
}

func (c *Cache) Start() error {
	return nil
}
