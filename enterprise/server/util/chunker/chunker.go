package chunker

import (
	"context"
	"io"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jotfs/fastcdc-go"
	"golang.org/x/sync/errgroup"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type WriteFunc func([]byte) error
type WriteChunkMetadataFunc func(*rfpb.FileRecord, filestore.PebbleKey, []byte) error
type GetFileRecordAndKeyForChunkFunc func([]byte) (*rfpb.FileRecord, filestore.PebbleKey, error)

type Chunker struct {
	ctx context.Context
	pw  *io.PipeWriter
	pr  *io.PipeReader

	done      chan struct{}
	closed    bool
	numChunks int

	eg         *errgroup.Group
	fileRecord *rfpb.FileRecord
	key        filestore.PebbleKey

	writtenChunks []*resource.ResourceName

	getFileRecordAndKeyForChunkFn GetFileRecordAndKeyForChunkFunc
	writeRawChunkFn               WriteChunkMetadataFunc
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

	if c.eg != nil {
		if err := c.eg.Wait(); err != nil {
			return err
		}
	}
	c.closed = true
	return nil
}

func (c *Chunker) WrittenChunks() ([]*resource.ResourceName, error) {
	if !c.closed {
		return nil, status.InternalErrorf("cannot call WrittenChunks() before chunker is closed")
	}
	return c.writtenChunks, nil
}

func (c *Chunker) writeFileWithOnlyOneChunk(chunkData []byte) error {
	return c.writeRawChunkFn(c.fileRecord, c.key, chunkData)
}

func (c *Chunker) writeChunk(chunkData []byte) error {
	fileRecord, key, err := c.getFileRecordAndKeyForChunkFn(chunkData)
	if err != nil {
		return err
	}
	r := &resource.ResourceName{
		Digest:         fileRecord.GetDigest(),
		InstanceName:   fileRecord.GetIsolation().GetRemoteInstanceName(),
		CacheType:      fileRecord.GetIsolation().GetCacheType(),
		DigestFunction: fileRecord.GetDigestFunction(),
		Compressor:     fileRecord.GetCompressor(),
	}
	c.writtenChunks = append(c.writtenChunks, r)

	c.eg.Go(func() error {
		return c.writeRawChunkFn(fileRecord, key, chunkData)
	})

	c.writeRawChunkFn(fileRecord, key, chunkData)
	return nil
}

// New returns an io.WriteCloser that split file into chunks of average size.
// averageSize is typically a power of 2. It must be in the range 64B to 1GiB.
// The minimum allowed chunk size is averageSize / 4, and the maximum allowed
// chunk size is averageSize * 4.
//
// singleChunkWriteFn and multipleChunkWriteFn specify how to write raw chunk
// when there is only one chunk and more than one chunks respectively
func New(ctx context.Context, averageSize int, fileRecord *rfpb.FileRecord, key filestore.PebbleKey, getFileRecordAndKeyForChunkFn GetFileRecordAndKeyForChunkFunc, writeRawChunkFn WriteChunkMetadataFunc) (*Chunker, error) {
	pr, pw := io.Pipe()
	c := &Chunker{
		ctx:                           ctx,
		pr:                            pr,
		pw:                            pw,
		done:                          make(chan struct{}, 0),
		fileRecord:                    fileRecord,
		key:                           key,
		getFileRecordAndKeyForChunkFn: getFileRecordAndKeyForChunkFn,
		writeRawChunkFn:               writeRawChunkFn,
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
				firstChunk = make([]byte, chunk.Length)
				copy(firstChunk, chunk.Data)
				continue
			} else if c.numChunks == 2 {
				bgCtx, _ := background.ExtendContextForFinalization(c.ctx, 5*time.Minute)
				eg, _ := errgroup.WithContext(bgCtx)
				c.eg = eg
				if err := c.writeChunk(firstChunk); err != nil {
					pr.CloseWithError(err)
					break
				}
			}

			var chunkData = make([]byte, chunk.Length)
			copy(chunkData, chunk.Data)
			if err := c.writeChunk(chunkData); err != nil {
				pr.CloseWithError(err)
				break
			}
		}

		if c.numChunks == 1 {
			if err := c.writeFileWithOnlyOneChunk(firstChunk); err != nil {
				pr.CloseWithError(err)
			}
		}

		close(c.done)
	}()

	return c, nil
}
