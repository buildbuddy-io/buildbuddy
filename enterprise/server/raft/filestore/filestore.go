// Package filestore implements io for reading bytestreams to/from pebble entries.
package filestore

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

// This should be the same or an evenly divisible value of
// the stream request size which is 1MB.
const (
	initialChunkNum    = 1
	maxPebbleValueSize = 3000000 // 3MB
)

type MetadataWriter interface {
	Metadata() *rfpb.StorageMetadata
}

type WriteCloserMetadata interface {
	io.Writer
	io.Closer
	MetadataWriter
}

func NewWriter(ctx context.Context, fileDir string, wb pebble.Writer, fileRecord *rfpb.FileRecord) (WriteCloserMetadata, error) {
	// New files are written using this method. Existing files will be read
	// from wherever they were originally written according to their stored
	// StorageMetadata.
	//
	// Uncommenting one of the following stanzas will cause newly written
	// files to be written either to pebble or to disk. Already written
	// files will be read from wherever they stored, regardless of this
	// setting.
	// return PebbleWriter(wb, fileRecord) NB: Pebble only writer needs more testing.
	return FileWriter(ctx, fileDir, fileRecord)
}

func NewReader(ctx context.Context, fileDir string, iter *pebble.Iterator, md *rfpb.StorageMetadata) (io.ReadCloser, error) {
	switch {
	case md.GetFileMetadata() != nil:
		return FileReader(ctx, fileDir, md.GetFileMetadata(), 0, 0)
	case md.GetPebbleMetadata() != nil:
		return PebbleReader(iter, md.GetPebbleMetadata()), nil
	default:
		return nil, status.InvalidArgumentErrorf("No stored metadata: %+v", md)
	}
}

type fileChunker struct {
	io.WriteCloser
	fileName string
}

func (c *fileChunker) Metadata() *rfpb.StorageMetadata {
	return &rfpb.StorageMetadata{
		FileMetadata: &rfpb.StorageMetadata_FileMetadata{
			Filename: c.fileName,
		},
	}
}

func FileReader(ctx context.Context, fileDir string, f *rfpb.StorageMetadata_FileMetadata, offset, limit int64) (io.ReadCloser, error) {
	return disk.FileReader(ctx, filepath.Join(fileDir, f.GetFilename()), offset, limit)
}

func FileWriter(ctx context.Context, fileDir string, fileRecord *rfpb.FileRecord) (WriteCloserMetadata, error) {
	file, err := constants.FileKey(fileRecord)
	if err != nil {
		return nil, err
	}
	wc, err := disk.FileWriter(ctx, filepath.Join(fileDir, string(file)))
	if err != nil {
		return nil, err
	}
	return &fileChunker{
		WriteCloser: wc,
		fileName:    string(file),
	}, nil
}

func chunkName(key []byte, idx int64) []byte {
	return append(key, []byte(strconv.FormatInt(idx, 10))...)
}

type pebbleChunker struct {
	wb       pebble.Writer
	key      keys.Key
	idx      int
	chunkNum int64
	buf      []byte
	closed   bool
}

// PebbleWriter takes a stream of data and writes it to sequential pebble KVs
// with a maximum size of maxPebbleValueSize. Chunks names begin at 1, and are
// equal to key + "-%d" where %d is the chunk number. StreamChunker implements
// the WriteCloser interface, but additionally implements a Metadata call,
// which returns a bit of metedata in proto form describing the data written.
func PebbleWriter(wb pebble.Writer, fr *rfpb.FileRecord) (WriteCloserMetadata, error) {
	key, err := constants.FileDataKey(fr)
	if err != nil {
		return nil, err
	}
	return &pebbleChunker{
		wb:       wb,
		key:      key,
		idx:      0,
		chunkNum: initialChunkNum,
		buf:      make([]byte, maxPebbleValueSize),
		closed:   false,
	}, nil
}

func (c *pebbleChunker) Write(data []byte) (int, error) {
	if c.closed {
		return 0, status.FailedPreconditionError("writer already closed")
	}
	dataReadPtr := 0
	for {
		n := cap(c.buf) - c.idx
		copied := copy(c.buf[c.idx:c.idx+n], data[dataReadPtr:])
		c.idx += copied
		dataReadPtr += copied
		if c.idx == cap(c.buf) {
			if err := c.flush(); err != nil {
				return 0, err
			}
		}
		if dataReadPtr == len(data) {
			break
		}
	}
	return dataReadPtr, nil
}

func (c *pebbleChunker) flush() error {
	if c.idx > 0 {
		if err := c.wb.Set(chunkName(c.key, c.chunkNum), c.buf[:c.idx], nil /*ignored write options*/); err != nil {
			return err
		}
		c.idx = 0
		c.chunkNum += 1
	}
	return nil
}

func (c *pebbleChunker) Close() error {
	if c.closed {
		return status.FailedPreconditionError("writer already closed")
	}
	if err := c.flush(); err != nil {
		return err
	}
	c.closed = true
	return nil
}

func (c *pebbleChunker) Metadata() *rfpb.StorageMetadata {
	if !c.closed {
		return nil
	}
	md := &rfpb.StorageMetadata{
		PebbleMetadata: &rfpb.StorageMetadata_PebbleMetadata{
			Key:    c.key,
			Chunks: c.chunkNum - initialChunkNum,
		},
	}
	return md
}

type pebbleStreamer struct {
	iter      *pebble.Iterator
	key       keys.Key
	idx       int64
	numChunks int64
	buf       []byte
}

// ChunkStreamer takes a pebble iterator and a key and reads sequential pebble
// KVS in a stream. Any missing chunks will cause an OutOfRangeError to be
// returned from Read().
func PebbleReader(iter *pebble.Iterator, p *rfpb.StorageMetadata_PebbleMetadata) io.ReadCloser {
	return &pebbleStreamer{
		iter:      iter,
		key:       p.GetKey(),
		idx:       initialChunkNum,
		numChunks: p.GetChunks(),
	}
}

func (c *pebbleStreamer) Read(buf []byte) (int, error) {
	copied := 0
	for {
		if len(c.buf) == 0 {
			if err := c.fetchNext(); err != nil {
				return copied, err
			}
		}
		n := copy(buf, c.buf)
		copied += n
		c.buf = c.buf[n:]
		if n == len(buf) {
			return copied, nil
		}
	}
}

func (c *pebbleStreamer) Close() error {
	return nil
}

// fetchNext is guaranteed to either return an error or
// fill c.buf.
func (c *pebbleStreamer) fetchNext() error {
	if c.idx >= c.numChunks+initialChunkNum {
		return io.EOF
	}
	chunk := chunkName(c.key, c.idx)
	found := c.iter.SeekGE(chunk)
	if !found || bytes.Compare(chunk, c.iter.Key()) != 0 {
		return status.OutOfRangeErrorf("chunk %q not found", chunkName(c.key, c.idx))
	}
	c.buf = make([]byte, len(c.iter.Value()))
	copy(c.buf, c.iter.Value())
	c.idx += 1
	return nil
}

func PebbleHasChunks(iter *pebble.Iterator, p *rfpb.StorageMetadata_PebbleMetadata) bool {
	for idx := int64(initialChunkNum); idx <= p.GetChunks(); idx++ {
		chunk := chunkName(p.GetKey(), idx)
		found := iter.SeekGE(chunk)
		if !found || bytes.Compare(chunk, iter.Key()) != 0 {
			return false
		}
	}
	return true
}
