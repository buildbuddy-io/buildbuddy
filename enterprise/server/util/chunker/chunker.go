package chunker

import (
	"context"
	"io"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jotfs/fastcdc-go"
)

type WriteFunc func([]byte) error

type Chunker struct {
	ctx context.Context
	pw  *io.PipeWriter
	pr  *io.PipeReader

	done chan struct{}

	mu  sync.Mutex // protexts err
	err error

	writeChunkFn WriteFunc
}

func (c *Chunker) Write(buf []byte) (int, error) {
	return c.pw.Write(buf)
}

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
		ctx:          ctx,
		pr:           pr,
		pw:           pw,
		done:         make(chan struct{}, 0),
		writeChunkFn: writeChunkFn,
	}
	cdcOpts := fastcdc.Options{
		AverageSize: averageSize,

		// Use the library default for MinSize and MaxSize. We explictly specified
		// the default here to avoid accident change of the values by the library.
		MinSize: averageSize / 4,
		MaxSize: averageSize * 4,

		// We want to keep the rolling hash the same to ensure that given the same
		// file, the library will chunk the file in the same way.
		Seed: 0,
	}

	chunker, err := fastcdc.NewChunker(pr, cdcOpts)
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
			if err := c.writeChunkFn(chunk.Data); err != nil {
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
		<-ctx.Done()
		c.mu.Lock()
		defer c.mu.Unlock()
		pr.CloseWithError(ctx.Err())
		if c.err == nil {
			c.err = ctx.Err()
		}
	}()

	return c, nil
}
