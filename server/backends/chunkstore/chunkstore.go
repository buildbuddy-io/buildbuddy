package chunkstore

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	mb   = 1 << 20
	year = time.Hour * 24 * 365
)

// This implements a chunking reader/writer interface on top of an arbitrary
// blobstore, averting the need to access blobs all at once.
type Chunkstore struct {
	internalBlobstore    interfaces.Blobstore
	writeBlockSize       int
	writeTimeoutDuration time.Duration
	noSplitWrite         bool
}

type ChunkstoreOptions struct {
	WriteBlockSize       int
	WriteTimeoutDuration time.Duration

	// If true, individual buffers passed to Write will not be split accross
	// multiple blobs unless the buffer itself exceeds the WriteBlockSize
	NoSplitWrite bool
}

func New(blobstore interfaces.Blobstore, co *ChunkstoreOptions) *Chunkstore {
	writeBlockSize := co.WriteBlockSize
	if writeBlockSize == 0 {
		writeBlockSize = 1 * mb
	}
	writeTimeoutDuration := co.WriteTimeoutDuration
	if writeTimeoutDuration == 0 {
		writeTimeoutDuration = 5 * time.Second
	}
	return &Chunkstore{
		internalBlobstore:    blobstore,
		writeBlockSize:       writeBlockSize,
		writeTimeoutDuration: writeTimeoutDuration,
		noSplitWrite:         co.NoSplitWrite,
	}
}

func (c *Chunkstore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	return c.ChunkExists(ctx, blobName, 0)
}

func (c *Chunkstore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	data, err := io.ReadAll(c.Reader(ctx, blobName))
	if err != nil {
		return []byte{}, err
	}
	return data, nil
}

func (c *Chunkstore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	c.DeleteBlob(ctx, blobName)
	w := c.Writer(ctx, blobName)
	bytesWritten, err := w.Write(data)
	if err != nil {
		return bytesWritten, err
	}
	bytesFlushed, err := w.Flush()
	if err != nil {
		return bytesWritten + bytesFlushed, err
	}

	return bytesWritten + bytesFlushed, w.Close()
}

func (c *Chunkstore) DeleteBlob(ctx context.Context, blobName string) error {
	index := uint16(0)
	for {
		if exists, err := c.ChunkExists(ctx, blobName, index); err != nil {
			return err
		} else if !exists {
			return nil
		}
		if err := c.deleteChunk(ctx, blobName, index); err != nil {
			return err
		}
		index++
	}
}

func ChunkIndexAsStringId(index uint16) string {
	return fmt.Sprintf("%04x", index)
}

func ChunkIdAsUint16Index(id string) (uint16, error) {
	n, err := strconv.ParseUint(id, 16, 16)
	if err != nil {
		return 0, err
	}
	return uint16(n), nil
}

func ChunkName(blobName string, index uint16) string {
	return blobName + "_" + ChunkIndexAsStringId(index)
}

func (c *Chunkstore) GetLastChunkId(ctx context.Context, blobName string, startingId string) (string, error) {
	startingIndex, err := ChunkIdAsUint16Index(startingId)
	if err != nil {
		return "", nil
	}
	index, err := c.getLastChunkIndex(ctx, blobName, startingIndex)
	return ChunkIndexAsStringId(index), err
}

func (c *Chunkstore) getLastChunkIndex(ctx context.Context, blobName string, startingIndex uint16) (uint16, error) {
	index := startingIndex
	if index == math.MaxUint16 {
		index = 0
	}
	for index < math.MaxUint16 {
		exists, err := c.ChunkExists(ctx, blobName, index)
		if err != nil {
			return 0, err
		}
		if !exists {
			if index == 0 || index == startingIndex {
				return math.MaxUint16, status.NotFoundErrorf("No Chunk found at index %d", index)
			}
			break
		}
		index++
	}
	return index - 1, nil
}

func (c *Chunkstore) ChunkExists(ctx context.Context, blobName string, index uint16) (bool, error) {
	return c.internalBlobstore.BlobExists(ctx, ChunkName(blobName, index))
}

func (c *Chunkstore) ReadChunk(ctx context.Context, blobName string, index uint16) ([]byte, error) {
	return c.internalBlobstore.ReadBlob(ctx, ChunkName(blobName, index))
}

func (c *Chunkstore) writeChunk(ctx context.Context, blobName string, index uint16, data []byte) (int, error) {
	return c.internalBlobstore.WriteBlob(ctx, ChunkName(blobName, index), data)
}

func (c *Chunkstore) deleteChunk(ctx context.Context, blobName string, index uint16) error {
	return c.internalBlobstore.DeleteBlob(ctx, ChunkName(blobName, index))
}

type chunkstoreReader struct {
	ctx        context.Context
	chunkstore *Chunkstore
	blobName   string
	chunk      []byte
	off        int64
	chunkOff   int
	chunkIndex uint16
	startIndex uint16
	reverse    bool
}

func (r *chunkstoreReader) advanceOffset(adv int64) {
	r.chunkOff += int(adv)
	r.off += adv
}

func (r *chunkstoreReader) nextChunkIndex() uint16 {
	if r.reverse {
		return r.chunkIndex - 1
	}
	return r.chunkIndex + 1
}

func (r *chunkstoreReader) noChunksRead() bool {
	return r.chunkIndex == r.startIndex
}

func (r *chunkstoreReader) copyAndAdvanceOffset(dst, src []byte) int {
	bytesRead := copy(dst, src)
	r.advanceOffset(int64(bytesRead))
	return bytesRead
}

func (r *chunkstoreReader) copyToReadBuffer(p []byte) int {
	if r.chunkOff >= len(r.chunk) {
		return 0
	}
	if r.reverse {
		remainingBytesInChunk := len(r.chunk) - r.chunkOff

		// If we intend to saturate our read buffer with this copy,
		// there will be no more bytes to read after the copy and
		// there will be len(p) fewer bytes remaining in the chunk
		remainingBytesInChunkAfterCopy := remainingBytesInChunk - len(p)
		bytesToReadAfterCopy := 0
		if len(p) > remainingBytesInChunk {
			// If we intend to copy the entire rest of the chunk into the
			// read buffer, there will be no bytes remaining in the chunk
			// after the copy, and the bytes left to read will be reduced
			// by the number of bytes currently remaining in the chunk
			bytesToReadAfterCopy = len(p) - remainingBytesInChunk
			remainingBytesInChunkAfterCopy = 0
		}
		return r.copyAndAdvanceOffset(p[bytesToReadAfterCopy:], r.chunk[remainingBytesInChunkAfterCopy:remainingBytesInChunk])
	}
	return r.copyAndAdvanceOffset(p, r.chunk[r.chunkOff:])
}

func (r *chunkstoreReader) nextChunkExists() (bool, error) {
	return r.chunkstore.ChunkExists(r.ctx, r.blobName, r.nextChunkIndex())
}

func (r *chunkstoreReader) getNextChunk() error {
	if exists, err := r.nextChunkExists(); err != nil {
		return err
	} else if !exists {
		return status.NotFoundErrorf("Opening %v: Couldn't find blob.", ChunkName(r.blobName, r.nextChunkIndex()))
	}
	r.chunkIndex = r.nextChunkIndex()
	// Decrementing the chunk offset by the length of the chunk instead
	// of zeroing it. This is important for the Seek operation.
	r.chunkOff -= len(r.chunk)
	var err error
	r.chunk, err = r.chunkstore.ReadChunk(r.ctx, r.blobName, r.chunkIndex)
	return err
}

func (r *chunkstoreReader) eof() (bool, error) {
	if r.chunkOff < len(r.chunk) {
		return false, nil
	}
	exists, err := r.nextChunkExists()
	return !exists, err
}

func (r *chunkstoreReader) shiftToFront(p []byte, bytesRead *int) {
	if *bytesRead < len(p) {
		copy(p, p[(len(p)-*bytesRead):])
		for i := *bytesRead; i < len(p); i++ {
			p[i] = 0
		}
	}
}

func (r *chunkstoreReader) Read(p []byte) (int, error) {
	bytesRead := r.copyToReadBuffer(p)
	if r.reverse {
		defer r.shiftToFront(p, &bytesRead)
	}
	for bytesRead < len(p) {
		err := r.getNextChunk()
		if r.reverse {
			//Exclude bytes already read in the read buffer
			bytesRead += r.copyToReadBuffer(p[:(len(p) - bytesRead)])
		} else {
			//Exclude bytes already read in the read buffer
			bytesRead += r.copyToReadBuffer(p[bytesRead:])
		}
		if err != nil {
			if status.IsNotFoundError(err) && !r.noChunksRead() {
				return bytesRead, io.EOF
			}
			return bytesRead, err
		}
	}
	if eof, err := r.eof(); err != nil {
		return bytesRead, err
	} else if eof {
		return bytesRead, io.EOF
	}
	return bytesRead, nil
}

func (r *chunkstoreReader) Seek(offset int64, whence int) (int64, error) {
	if offset == r.off {
		return r.off, nil
	}
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset = r.off + offset
	case io.SeekEnd:
		for {
			if exists, err := r.nextChunkExists(); err != nil {
				return r.off, err
			} else if !exists {
				break
			}
			r.getNextChunk()
		}
		offset += r.off + int64(len(r.chunk)-r.chunkOff)
	default:
		return r.off, syscall.EINVAL
	}

	if offset < 0 {
		return r.off, syscall.EINVAL
	}

	// If the requested offset is within the current chunk, just change the offset and return
	if offset >= r.off-int64(r.chunkOff) && offset < r.off+int64(len(r.chunk)-r.chunkOff) {
		r.advanceOffset(offset - r.off)
		return r.off, nil
	}

	// If the offset is before the current offset (and not within the current chunk),
	// reset the offset too the beginning of the file
	if offset < r.off {
		r.chunkIndex = r.startIndex
		r.chunk = []byte{}
		r.off = 0
		r.chunkOff = 0
	}
	r.advanceOffset(offset - r.off)
	return r.off, nil
}

func (r *chunkstoreReader) Close() error {
	r.blobName = ""
	r.chunk = nil
	r.ctx = nil
	r.chunkstore = nil
	return nil
}

func (c *Chunkstore) Reader(ctx context.Context, blobName string) *chunkstoreReader {
	return &chunkstoreReader{
		chunkstore: c,
		chunkIndex: math.MaxUint16,
		startIndex: math.MaxUint16,
		chunk:      []byte{},
		blobName:   blobName,
		ctx:        ctx,
	}
}

func (c *Chunkstore) ReverseReader(ctx context.Context, blobName string) (*chunkstoreReader, error) {
	chunkIndex := uint16(0)
	for {
		if exists, err := c.ChunkExists(ctx, blobName, chunkIndex); err != nil {
			return nil, err
		} else if !exists {
			break
		}
		chunkIndex++
	}
	return &chunkstoreReader{
		chunkstore: c,
		chunkIndex: chunkIndex,
		startIndex: chunkIndex,
		chunk:      []byte{},
		blobName:   blobName,
		ctx:        ctx,
		reverse:    true,
	}, nil
}

type writeResult struct {
	err            error
	size           int
	lastChunkIndex uint16
}

type ChunkstoreWriter struct {
	chunkstore         *Chunkstore
	blobName           string
	lastChunkIndex     uint16
	writeChannel       chan []byte
	writeResultChannel chan writeResult
	closed             bool
}

func (w *ChunkstoreWriter) readFromWriteResultChannel() (int, error) {
	select {
	case result, open := <-w.writeResultChannel:
		if !open {
			if !w.closed {
				close(w.writeChannel)
				w.closed = true
			}
			return 0, status.UnavailableErrorf("Error accessing %v: Already closed.", w.blobName)
		}
		w.lastChunkIndex = result.lastChunkIndex
		return result.size, result.err
	case <-time.After(w.chunkstore.writeTimeoutDuration):
		return 0, status.DeadlineExceededErrorf("Error accessing %v: Deadline exceeded.", w.blobName)
	}
}

func (w *ChunkstoreWriter) GetLastChunkIndex() uint16 {
	// Call Write with an empty buffer to update lastChunkIndex without triggering
	// an actual write to blobstore, which lets us get an updated index in case a
	// timeout-based flush has happened between now and the last call to Write.
	w.Write([]byte{})
	return w.lastChunkIndex
}

func (w *ChunkstoreWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, nil
	}
	w.writeChannel <- p
	return w.readFromWriteResultChannel()
}

func (w *ChunkstoreWriter) Flush() (int, error) {
	return w.Write(nil)
}

func (w *ChunkstoreWriter) Close() error {
	if w.closed {
		return nil
	}
	close(w.writeChannel)
	_, err := w.readFromWriteResultChannel()
	w.closed = true
	return err
}

type writeLoop struct {
	chunkstore         *Chunkstore
	blobName           string
	ctx                context.Context
	chunkIndex         uint16
	chunk              []byte
	flushTime          time.Time
	writeError         error
	bytesFlushed       int
	open               bool
	timeout            bool
	writeChannel       chan []byte
	writeResultChannel chan writeResult
	lastWriteSize      int
}

func (l *writeLoop) write() error {
	size := l.chunkstore.writeBlockSize
	if len(l.chunk) <= size {
		size = len(l.chunk)
	} else if l.chunkstore.noSplitWrite && l.lastWriteSize <= l.chunkstore.writeBlockSize {
		size = len(l.chunk) - l.lastWriteSize
	}
	bytesWritten, err := l.chunkstore.writeChunk(l.ctx, l.blobName, l.chunkIndex, l.chunk[:size])
	if bytesWritten > 0 {
		l.bytesFlushed += size
		l.chunk = l.chunk[size:]
		l.chunkIndex++
	}
	if err != nil {
		l.writeError = err
	}
	return err
}

func (l *writeLoop) readFromWriteChannel() {
	l.timeout = false
	var p []byte
	select {
	case p, l.open = <-l.writeChannel:
		if l.open && p == nil {
			l.flushTime = time.Unix(0, 0)
		} else {
			l.chunk = append(l.chunk, p...)
			l.lastWriteSize = len(p)
		}
	case <-time.After(time.Until(l.flushTime)):
		l.timeout = true
	case <-l.ctx.Done():
		l.open = false
	}
}

func (l *writeLoop) run() {
	l.flushTime = time.Now().Add(year)
	l.open = true
	for l.open {
		l.readFromWriteChannel()
		var err error
		for err == nil && len(l.chunk) >= l.chunkstore.writeBlockSize {
			err = l.write()
		}
		if err == nil && ((!l.open && l.chunkIndex == 0) || (len(l.chunk) > 0 && (time.Now().After(l.flushTime) || !l.open))) {
			l.write()
		}
		if len(l.chunk) == 0 {
			l.flushTime = time.Now().Add(year)
		} else if time.Until(l.flushTime) > (l.chunkstore.writeTimeoutDuration) {
			l.flushTime = time.Now().Add(l.chunkstore.writeTimeoutDuration)
		}
		if l.timeout {
			continue
		}
		l.writeResultChannel <- writeResult{size: l.bytesFlushed, err: l.writeError, lastChunkIndex: l.chunkIndex - 1}
		l.writeError = nil
		l.bytesFlushed = 0
	}
	close(l.writeResultChannel)
}

func (c *Chunkstore) Writer(ctx context.Context, blobName string) *ChunkstoreWriter {
	writer := &ChunkstoreWriter{
		chunkstore:         c,
		blobName:           blobName,
		writeChannel:       make(chan []byte),
		writeResultChannel: make(chan writeResult),
	}

	loop := &writeLoop{
		chunkstore:         c,
		ctx:                ctx,
		blobName:           blobName,
		writeChannel:       writer.writeChannel,
		writeResultChannel: writer.writeResultChannel,
	}

	go loop.run()

	return writer
}
