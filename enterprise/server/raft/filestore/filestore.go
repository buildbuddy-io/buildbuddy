// Package filestore implements io for reading bytestreams to/from pebble entries.
package filestore

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"path/filepath"
	"strconv"
	"time"

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

// returns partitionID, isolation, hash or
// returns partitionID, isolation, remote_instance_name, hahs
func fileRecordSegments(r *rfpb.FileRecord) (partID string, isolation string, remoteInstanceHash string, digestHash string, err error) {
	if r.GetIsolation().GetPartitionId() == "" {
		err = status.FailedPreconditionError("Empty partition ID not allowed in filerecord.")
		return
	}
	partID = r.GetIsolation().GetPartitionId()

	if r.GetIsolation().GetCacheType() == rfpb.Isolation_CAS_CACHE {
		isolation = "cas"
	} else if r.GetIsolation().GetCacheType() == rfpb.Isolation_ACTION_CACHE {
		isolation = "ac"
		if remoteInstanceName := r.GetIsolation().GetRemoteInstanceName(); remoteInstanceName != "" {
			remoteInstanceHash = strconv.Itoa(int(crc32.ChecksumIEEE([]byte(remoteInstanceName))))
		}
	} else {
		err = status.FailedPreconditionError("Isolation type must be explicitly set, not UNKNOWN.")
		return
	}
	if len(r.GetDigest().GetHash()) <= 4 {
		err = status.FailedPreconditionError("Malformed digest; too short.")
		return
	}
	digestHash = r.GetDigest().GetHash()
	return
}

// FileKey is the partial path where a file will be written.
// For example, given a fileRecord with FileKey: "foo/bar", the filestore will
// write the file at a path like "/root/dir/blobs/foo/bar".
func FileKey(r *rfpb.FileRecord) ([]byte, error) {
	// This function cannot change without a data migration.
	// filekeys look like this:
	//   // {groupID}/{ac|cas}/{hashPrefix:4}/{hash}
	//   // for example:
	//   //   PART123/ac/44321/abcd/abcd12345asdasdasd123123123asdasdasd
	//   //   PART123/cas/abcd/abcd12345asdasdasd123123123asdasdasd
	partID, isolation, remoteInstanceHash, hash, err := fileRecordSegments(r)
	if err != nil {
		return nil, err
	}

	return []byte(filepath.Join(partID, isolation, remoteInstanceHash, hash[:4], hash)), nil
}

// FileDataKey is the partial key name where a file will be written if it is
// stored entirely in pebble.
// For example, given a fileRecord with FileKey: "tiny/file", the filestore will
// write the file under pebble keys like:
//   - tiny/file-0
//   - tiny/file-1
//   - tiny/file-2
func FileDataKey(r *rfpb.FileRecord) ([]byte, error) {
	// This function cannot change without a data migration.
	// File Data keys look like this:
	//   // {groupID}/{ac|cas}/{hash}-
	//   // for example:
	//   //   PART123/ac/44321/abcd12345asdasdasd123123123asdasdasd-
	//   //   PART123/cas/abcd12345asdasdasd123123123asdasdasd-
	partID, isolation, remoteInstanceHash, hash, err := fileRecordSegments(r)
	if err != nil {
		return nil, err
	}
	return []byte(filepath.Join(partID, isolation, remoteInstanceHash, hash) + "-"), nil
}

// FileMetadataKey is the partial key name where a file's metadata will be
// written in pebble.
// For example, given a fileRecord with FileMetadataKey: "baz/bap", the filestore will
// write the file's metadata under pebble key like:
//   - baz/bap
func FileMetadataKey(r *rfpb.FileRecord) ([]byte, error) {
	// This function cannot change without a data migration.
	// Metadata keys look like this:
	//   // {groupID}/{ac|cas}/{hash}
	//   // for example:
	//   //   PART123456/ac/44321/abcd12345asdasdasd123123123asdasdasd
	//   //   PART123456/cas/abcd12345asdasdasd123123123asdasdasd
	partID, isolation, remoteInstanceHash, hash, err := fileRecordSegments(r)
	if err != nil {
		return nil, err
	}
	return []byte(filepath.Join(partID, isolation, remoteInstanceHash, hash)), nil
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

func NewReader(ctx context.Context, fileDir string, md *rfpb.StorageMetadata, offset, limit int64) (io.ReadCloser, error) {
	switch {
	case md.GetFileMetadata() != nil:
		return FileReader(ctx, fileDir, md.GetFileMetadata(), offset, limit)
	case md.GetInlineMetadata() != nil:
		return InlineReader(md.GetInlineMetadata(), offset, limit)
	default:
		return nil, status.InvalidArgumentErrorf("No stored metadata: %+v", md)
	}
}

func InlineReader(f *rfpb.StorageMetadata_InlineMetadata, offset, limit int64) (io.ReadCloser, error) {
	r := bytes.NewReader(f.GetData())
	r.Seek(offset, 0)
	length := int64(len(f.GetData()))
	if limit != 0 && limit < length {
		length = limit
	}
	if length > 0 {
		return io.NopCloser(io.LimitReader(r, length)), nil
	}
	return io.NopCloser(r), nil
}

type inlineWriter struct {
	*bytes.Buffer
}

func (iw *inlineWriter) Close() error {
	return nil
}

func (iw *inlineWriter) Metadata() *rfpb.StorageMetadata {
	return &rfpb.StorageMetadata{
		InlineMetadata: &rfpb.StorageMetadata_InlineMetadata{
			Data:          iw.Buffer.Bytes(),
			CreatedAtNsec: time.Now().UnixNano(),
		},
	}
}

func InlineWriter(ctx context.Context, sizeBytes int64) WriteCloserMetadata {
	return &inlineWriter{bytes.NewBuffer(make([]byte, 0, sizeBytes))}
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

func FilePath(fileDir string, f *rfpb.StorageMetadata_FileMetadata) string {
	fp := f.GetFilename()
	if !filepath.IsAbs(fp) {
		fp = filepath.Join(fileDir, f.GetFilename())
	}
	return fp
}

func FileReader(ctx context.Context, fileDir string, f *rfpb.StorageMetadata_FileMetadata, offset, limit int64) (io.ReadCloser, error) {
	fp := FilePath(fileDir, f)
	return disk.FileReader(ctx, fp, offset, limit)
}

func FileWriter(ctx context.Context, fileDir string, fileRecord *rfpb.FileRecord) (WriteCloserMetadata, error) {
	file, err := FileKey(fileRecord)
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
	key, err := FileDataKey(fr)
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
