package chunker

import (
	"context"
	"io"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/fastcdc2020/fastcdc"
)

type WriteFunc func([]byte) error

type Chunker struct {
	pw *io.PipeWriter

	done chan struct{}

	mu  sync.Mutex // protects err
	err error
}

func (c *Chunker) Write(buf []byte) (int, error) {
	return c.pw.Write(buf)
}

// Close blocks until all chunks have been processed.
func (c *Chunker) Close() error {
	if err := c.pw.Close(); err != nil {
		return status.InternalErrorf("failed to close chunker: %s", err)
	}

	<-c.done

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// New returns an io.WriteCloser that split file into chunks of average size.
// averageSize is typically a power of 2. It must be in the range 256B to 256MB.
// The minimum allowed chunk size is averageSize / 4, and the maximum allowed
// chunk size is averageSize * 4.
func New(ctx context.Context, averageSize int, writeChunkFn WriteFunc) (*Chunker, error) {
	pr, pw := io.Pipe()
	c := &Chunker{
		pw:   pw,
		done: make(chan struct{}),
	}

	chunker, err := fastcdc.NewChunker(
		pr,
		averageSize,
		fastcdc.WithMinSize(averageSize/4),
		fastcdc.WithMaxSize(averageSize*4),
		fastcdc.WithSeed(0),
		fastcdc.WithNormalization(2),
	)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(c.done)
		for {
			chunk, err := chunker.Next()
			if err == io.EOF {
				return
			}
			if err != nil {
				err = status.InternalErrorf("failed to get the next chunk: %s", err)
				pr.CloseWithError(err)
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.err == nil {
					c.err = err
				}
				return
			}
			if err := writeChunkFn(chunk.Data); err != nil {
				err = status.InternalErrorf("writeChunkFn failed: %s", err)
				pr.CloseWithError(err)
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.err == nil {
					c.err = err
				}
				return
			}
		}
	}()

	go func() {
		select {
		case <-c.done:
			return
		case <-ctx.Done():
		}
		pr.CloseWithError(ctx.Err())
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.err == nil {
			c.err = ctx.Err()
		}
	}()

	return c, nil
}
