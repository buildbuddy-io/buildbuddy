package chunker

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jotfs/fastcdc-go"
)

type WriteFunc func([]byte) error

type Chunker struct {
	ctx context.Context
	pw  *io.PipeWriter
	pr  *io.PipeReader

	done      chan struct{}
	closed    bool
	numChunks int

	singleChunkWriteFn   WriteFunc
	multipleChunkWriteFn WriteFunc
}

func (c *Chunker) Write(buf []byte) (int, error) {
	return c.pw.Write(buf)
}

func (c *Chunker) Close() error {
	if err := c.pw.Close(); err != nil {
		return err
	}

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-c.done:
		break
	}
	c.closed = true
	return nil
}

func (c *Chunker) NumChunks() (int, error) {
	if !c.closed {
		return 0, status.InternalErrorf("cannot call NumChunks() before chunker is closed")
	}
	return c.numChunks, nil
}

// New returns an io.WriteCloser that split file into chunks of average size.
// averageSize is typically a power of 2. It must be in the range 64B to 1GiB.
// The minimum allowed chunk size is averageSize / 4, and the maximum allowed
// chunk size is averageSize * 4.
//
// singleChunkWriteFn and multipleChunkWriteFn specify how to write raw chunk
// when there is only one chunk and more than one chunks respectively
func New(ctx context.Context, averageSize int, singleChunkWriteFn WriteFunc, multipleChunkWriteFn WriteFunc) (*Chunker, error) {
	pr, pw := io.Pipe()
	c := &Chunker{
		ctx:                  ctx,
		pr:                   pr,
		pw:                   pw,
		done:                 make(chan struct{}, 0),
		singleChunkWriteFn:   singleChunkWriteFn,
		multipleChunkWriteFn: multipleChunkWriteFn,
	}
	cdcOpts := fastcdc.Options{
		MinSize:     averageSize / 4,
		AverageSize: averageSize,
		MaxSize:     averageSize * 4,
	}

	chunker, err := fastcdc.NewChunker(pr, cdcOpts)
	if err != nil {
		return nil, err
	}

	go func() {
		var firstChunk []byte
		for {
			chunk, err := chunker.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				pr.CloseWithError(err)
				break
			}
			c.numChunks++

			if c.numChunks == 1 {
				// We don't know whether there will be one chunk or multiple
				// chunks. Wait to write.
				firstChunk = make([]byte, len(chunk.Data))
				copy(firstChunk, chunk.Data)
				continue
			} else if c.numChunks == 2 {
				if err := c.multipleChunkWriteFn(firstChunk); err != nil {
					pr.CloseWithError(err)
					break
				}
			}

			if err := c.multipleChunkWriteFn(chunk.Data); err != nil {
				pr.CloseWithError(err)
				break
			}
		}

		if c.numChunks == 1 {
			if err := c.singleChunkWriteFn(firstChunk); err != nil {
				pr.CloseWithError(err)
			}
		}

		close(c.done)
	}()

	return c, nil
}
