package composable_cache

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

type CacheMode uint32

const (
	ModeReadThrough  CacheMode = 1 << (32 - 1 - iota) // Outer cache value set after successful read from inner
	ModeWriteThrough                                  // Outer cache value set after successful write to inner
)

type ComposableCache struct {
	inner interfaces.Cache
	outer interfaces.Cache
	mode  CacheMode
}

func NewComposableCache(outer, inner interfaces.Cache, mode CacheMode) interfaces.Cache {
	return &ComposableCache{
		inner: inner,
		outer: outer,
		mode:  mode,
	}
}

func (c *ComposableCache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	outerExists, err := c.outer.Contains(ctx, r)
	if err == nil && outerExists {
		return outerExists, nil
	}

	return c.inner.Contains(ctx, r)
}

func (c *ComposableCache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	md, err := c.outer.Metadata(ctx, r)
	if err == nil {
		return md, nil
	}
	return c.inner.Metadata(ctx, r)
}

func (c *ComposableCache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	if len(resources) == 0 {
		return nil, nil
	}
	cacheType := resources[0].GetCacheType()
	instanceName := resources[0].GetInstanceName()

	missingDigests, err := c.outer.FindMissing(ctx, resources)
	missingResources := digest.ResourceNames(cacheType, instanceName, missingDigests)
	if err != nil {
		missingResources = resources
	}
	if len(missingResources) == 0 {
		return nil, nil
	}
	return c.inner.FindMissing(ctx, missingResources)
}

func (c *ComposableCache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	outerRsp, err := c.outer.Get(ctx, r)
	if err == nil {
		return outerRsp, nil
	}

	innerRsp, err := c.inner.Get(ctx, r)
	if err != nil {
		return nil, err
	}
	if c.mode&ModeReadThrough != 0 {
		c.outer.Set(ctx, r, innerRsp)
	}

	return innerRsp, nil
}

func (c *ComposableCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	if len(resources) == 0 {
		return nil, nil
	}
	cacheType := resources[0].GetCacheType()
	instanceName := resources[0].GetInstanceName()

	foundMap := make(map[*repb.Digest][]byte, len(resources))
	if outerFoundMap, err := c.outer.GetMulti(ctx, resources); err == nil {
		for d, data := range outerFoundMap {
			foundMap[d] = data
		}
	}
	stillMissing := make([]*rspb.ResourceName, 0)
	for _, r := range resources {
		d := r.GetDigest()
		if _, ok := foundMap[d]; !ok {
			stillMissing = append(stillMissing, &rspb.ResourceName{
				Digest:       d,
				InstanceName: instanceName,
				CacheType:    cacheType,
			})
		}
	}
	if len(stillMissing) == 0 {
		return foundMap, nil
	}

	innerFoundMap, err := c.inner.GetMulti(ctx, stillMissing)
	if err != nil {
		return nil, err
	}
	for d, data := range innerFoundMap {
		foundMap[d] = data
	}
	return foundMap, nil
}

func (c *ComposableCache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	// Special case -- we call set on the inner cache first (in case of
	// error) and then if no error we'll maybe set on the outer.
	if err := c.inner.Set(ctx, r, data); err != nil {
		return err
	}
	if c.mode&ModeWriteThrough != 0 {
		c.outer.Set(ctx, r, data)
	}
	return nil
}

func (c *ComposableCache) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
	if err := c.inner.SetMulti(ctx, kvs); err != nil {
		return err
	}
	if c.mode&ModeWriteThrough != 0 {
		c.outer.SetMulti(ctx, kvs)
	}
	return nil
}

func (c *ComposableCache) Delete(ctx context.Context, r *rspb.ResourceName) error {
	// Special case -- we call delete on the inner cache first (in case of
	// error) and then if no error we'll maybe delete from the outer.
	if err := c.inner.Delete(ctx, r); err != nil {
		return err
	}
	if c.mode&ModeWriteThrough != 0 {
		c.outer.Delete(ctx, r)
	}
	return nil
}

// TeeReader returns a Reader that writes to w what it reads from r.
// All reads from r performed through it are matched with
// corresponding writes to w. There is no internal buffering -
// the write must complete before the read completes.
// Any error encountered while writing is *ignored*.
// Once the reader encounters an EOF, the writer is closed.
type ReadCloser struct {
	io.Reader
	io.Closer
}

type MultiCloser struct {
	closers []io.Closer
}

func (m *MultiCloser) Close() error {
	for _, c := range m.closers {
		err := c.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ComposableCache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	if outerReader, err := c.outer.Reader(ctx, r, uncompressedOffset, limit); err == nil {
		return outerReader, nil
	}

	innerReader, err := c.inner.Reader(ctx, r, uncompressedOffset, limit)
	if err != nil {
		return nil, err
	}

	if c.mode&ModeReadThrough == 0 || uncompressedOffset != 0 {
		return innerReader, nil
	}

	// Copy the digest over to the outer cache.
	outerWriter, err := c.outer.Writer(ctx, r)
	// Directly return the inner reader if the outer cache doesn't want the
	// blob.
	if err != nil {
		return innerReader, nil
	}
	defer outerWriter.Close()
	if _, err := io.Copy(outerWriter, innerReader); err != nil {
		return nil, err
	}
	// We're done with the inner reader at this point, we'll create a new
	// reader below.
	innerReader.Close()
	if err := outerWriter.Commit(); err != nil {
		return nil, err
	}
	outerReader, err := c.outer.Reader(ctx, r, uncompressedOffset, limit)
	if err != nil {
		return nil, err
	}
	return outerReader, nil
}

type doubleWriter struct {
	inner    interfaces.CommittedWriteCloser
	outer    interfaces.CommittedWriteCloser
	commitFn func(err error)
}

func (d *doubleWriter) Write(p []byte) (int, error) {
	n, err := d.inner.Write(p)
	if err != nil {
		return n, err
	}
	if n > 0 {
		if n, err := d.outer.Write(p); err != nil {
			return n, err
		}
	}
	return n, err
}

func (d *doubleWriter) Commit() error {
	err := d.inner.Commit()
	d.commitFn(err)
	return err
}

func (d *doubleWriter) Close() error {
	err := d.inner.Close()
	d.outer.Close()
	return err
}

func (c *ComposableCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	innerWriter, err := c.inner.Writer(ctx, r)
	if err != nil {
		return nil, err
	}

	if c.mode&ModeWriteThrough != 0 {
		if outerWriter, err := c.outer.Writer(ctx, r); err == nil {
			dw := &doubleWriter{
				inner: innerWriter,
				outer: outerWriter,
				commitFn: func(err error) {
					if err == nil {
						outerWriter.Commit()
					}
				},
			}
			return dw, nil
		}
	}

	return innerWriter, nil
}

func (c *ComposableCache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	return compressor == repb.Compressor_IDENTITY
}
