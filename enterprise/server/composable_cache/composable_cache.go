package composable_cache

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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

func (c *ComposableCache) WithIsolation(ctx context.Context, cacheType resource.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	newInner, err := c.inner.WithIsolation(ctx, cacheType, remoteInstanceName)
	if err != nil {
		return nil, status.WrapError(err, "WithIsolation failed on inner cache")
	}
	newOuter, err := c.outer.WithIsolation(ctx, cacheType, remoteInstanceName)
	if err != nil {
		return nil, status.WrapError(err, "WithIsolation failed on outer cache")
	}
	return &ComposableCache{
		inner: newInner,
		outer: newOuter,
		mode:  c.mode,
	}, nil
}

func (c *ComposableCache) Contains(ctx context.Context, r *resource.ResourceName) (bool, error) {
	outerExists, err := c.outer.Contains(ctx, r)
	if err == nil && outerExists {
		return outerExists, nil
	}

	return c.inner.Contains(ctx, r)
}

func (c *ComposableCache) ContainsDeprecated(ctx context.Context, d *repb.Digest) (bool, error) {
	outerExists, err := c.outer.ContainsDeprecated(ctx, d)
	if err == nil && outerExists {
		return outerExists, nil
	}

	return c.inner.ContainsDeprecated(ctx, d)
}

func (c *ComposableCache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	md, err := c.outer.Metadata(ctx, d)
	if err == nil {
		return md, nil
	}
	return c.inner.Metadata(ctx, d)
}

func (c *ComposableCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	missing, err := c.outer.FindMissing(ctx, digests)
	if err != nil {
		missing = digests
	}
	if len(missing) == 0 {
		return nil, nil
	}
	return c.inner.FindMissing(ctx, missing)
}

func (c *ComposableCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	outerRsp, err := c.outer.Get(ctx, d)
	if err == nil {
		return outerRsp, nil
	}

	innerRsp, err := c.inner.Get(ctx, d)
	if err != nil {
		return nil, err
	}
	if c.mode&ModeReadThrough != 0 {
		c.outer.Set(ctx, d, innerRsp)
	}

	return innerRsp, nil
}

func (c *ComposableCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	foundMap := make(map[*repb.Digest][]byte, len(digests))
	if outerFoundMap, err := c.outer.GetMulti(ctx, digests); err == nil {
		for d, data := range outerFoundMap {
			foundMap[d] = data
		}
	}
	stillMissing := make([]*repb.Digest, 0)
	for _, d := range digests {
		if _, ok := foundMap[d]; !ok {
			stillMissing = append(stillMissing, d)
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

func (c *ComposableCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	// Special case -- we call set on the inner cache first (in case of
	// error) and then if no error we'll maybe set on the outer.
	if err := c.inner.Set(ctx, d, data); err != nil {
		return err
	}
	if c.mode&ModeWriteThrough != 0 {
		c.outer.Set(ctx, d, data)
	}
	return nil
}

func (c *ComposableCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	if err := c.inner.SetMulti(ctx, kvs); err != nil {
		return err
	}
	if c.mode&ModeWriteThrough != 0 {
		c.outer.SetMulti(ctx, kvs)
	}
	return nil
}

func (c *ComposableCache) Delete(ctx context.Context, d *repb.Digest) error {
	// Special case -- we call delete on the inner cache first (in case of
	// error) and then if no error we'll maybe delete from the outer.
	if err := c.inner.Delete(ctx, d); err != nil {
		return err
	}
	if c.mode&ModeWriteThrough != 0 {
		c.outer.Delete(ctx, d)
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

func (c *ComposableCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	if outerReader, err := c.outer.Reader(ctx, d, offset, limit); err == nil {
		return outerReader, nil
	}

	innerReader, err := c.inner.Reader(ctx, d, offset, limit)
	if err != nil {
		return nil, err
	}

	if c.mode&ModeReadThrough == 0 || offset != 0 {
		return innerReader, nil
	}

	// Copy the digest over to the outer cache.

	outerWriter, err := c.outer.Writer(ctx, d)
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
	outerReader, err := c.outer.Reader(ctx, d, offset, limit)
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

func (c *ComposableCache) Writer(ctx context.Context, d *repb.Digest) (interfaces.CommittedWriteCloser, error) {
	innerWriter, err := c.inner.Writer(ctx, d)
	if err != nil {
		return nil, err
	}

	if c.mode&ModeWriteThrough != 0 {
		if outerWriter, err := c.outer.Writer(ctx, d); err == nil {
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
