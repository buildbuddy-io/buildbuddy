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
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
)

const (
	mb         = 1 << 20
	year       = time.Hour * 24 * 365
	EmptyIndex = math.MaxUint16
)

// This implements a chunking reader/writer interface on top of an arbitrary
// blobstore, averting the need to access blobs all at once.
type Chunkstore struct {
	internalBlobstore interfaces.Blobstore
	writeBlockSize    int
}

type ChunkstoreOptions struct {
	WriteBlockSize int
}

func New(blobstore interfaces.Blobstore, co *ChunkstoreOptions) *Chunkstore {
	writeBlockSize := 1 * mb
	if co != nil && co.WriteBlockSize != 0 {
		writeBlockSize = co.WriteBlockSize
	}
	return &Chunkstore{
		internalBlobstore: blobstore,
		writeBlockSize:    writeBlockSize,
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
	if err := c.DeleteBlob(ctx, blobName); err != nil {
		return 0, err
	}
	w := c.Writer(ctx, blobName, nil)
	bytesWritten, err := w.Write(ctx, data)
	if err != nil {
		return bytesWritten, err
	}
	bytesFlushed, err := w.Flush(ctx)
	if err != nil {
		return bytesWritten + bytesFlushed, err
	}

	return bytesWritten + bytesFlushed, w.Close(ctx)
}

func (c *Chunkstore) DeleteBlob(ctx context.Context, blobName string) error {
	// delete blobs from back to front so that this process is recoverable in case
	// of failure (delete error, server crash, etc.)
	i := uint16(math.MaxUint16)
	for {
		if exists, err := c.ChunkExists(ctx, blobName, i+1); err != nil {
			return err
		} else if !exists {
			break
		}
		i++
	}
	for i != uint16(math.MaxUint16) {
		if err := c.deleteChunk(ctx, blobName, i); err != nil {
			return err
		}
		i--
	}
	return nil
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
	// reset the offset to the beginning of the file
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

type WriteRequest struct {
	ctx          context.Context
	Chunk        []byte
	VolatileTail []byte
	Close        bool
}

func (w *WriteRequest) IsEmpty() bool {
	return w == nil || ((w.Chunk == nil || len(w.Chunk) == 0) && w.VolatileTail == nil && !w.Close)
}

type WriteResult struct {
	Err            error
	Size           int
	LastChunkIndex uint16
	Timeout        bool
	BytesFlushed   int
	Close          bool
}

type ChunkstoreWriterOptions struct {
	WriteHook            func(context.Context, *WriteRequest, *WriteResult, []byte, []byte)
	WriteBlockSize       int
	WriteTimeoutDuration time.Duration
	NoSplitWrite         bool
}

type ChunkstoreWriter struct {
	writeChannel         chan *WriteRequest
	writeResultChannel   chan *WriteResult
	blobName             string
	writeTimeoutDuration time.Duration
	lastChunkIndex       uint16
	closed               bool
}

func (w *ChunkstoreWriter) readFromWriteResultChannel() (int, error) {
	delay := time.NewTimer(w.writeTimeoutDuration)
	// don't leak the timer
	defer timeutil.StopAndDrainTimer(delay)
	select {
	case result, open := <-w.writeResultChannel:
		if !open || result.Close {
			close(w.writeChannel)
			w.closed = true
			if result != nil && result.Err != nil {
				return 0, result.Err
			}
			return 0, status.UnavailableErrorf("Error accessing %v: Already closed.", w.blobName)
		}
		w.lastChunkIndex = result.LastChunkIndex
		if result.Timeout {
			return w.readFromWriteResultChannel()
		}
		return result.Size, result.Err
	case <-delay.C:
		return 0, status.DeadlineExceededErrorf("Error accessing %v: Deadline exceeded.", w.blobName)
	}
}

func (w *ChunkstoreWriter) GetLastChunkIndex(ctx context.Context) uint16 {
	// Call Write with an empty buffer to update lastChunkIndex without triggering
	// an actual write to blobstore, which lets us get an updated index in case a
	// timeout-based flush has happened between now and the last call to Write.
	w.WriteWithTail(ctx, []byte{}, nil)
	return w.lastChunkIndex
}

func (w *ChunkstoreWriter) Write(ctx context.Context, p []byte) (int, error) {
	return w.WriteWithTail(ctx, p, []byte{})
}

func (w *ChunkstoreWriter) WriteWithTail(ctx context.Context, p []byte, tail []byte) (int, error) {
	if w.closed {
		return 0, nil
	}
	w.writeChannel <- &WriteRequest{ctx: ctx, Chunk: p, VolatileTail: tail}
	return w.readFromWriteResultChannel()
}

func (w *ChunkstoreWriter) Flush(ctx context.Context) (int, error) {
	return w.Write(ctx, nil)
}

func (w *ChunkstoreWriter) Close(ctx context.Context) error {
	if w.closed {
		return nil
	}
	w.writeChannel <- &WriteRequest{ctx: ctx, Close: true}
	_, err := w.readFromWriteResultChannel()
	if status.IsUnavailableError(err) {
		return nil
	}
	return err
}

type writeLoop struct {
	writeResultChannel   chan *WriteResult
	writeChannel         chan *WriteRequest
	chunkstore           *Chunkstore
	writeHook            func(context.Context, *WriteRequest, *WriteResult, []byte, []byte)
	blobName             string
	writeBlockSize       int
	writeTimeoutDuration time.Duration
	noSplitWrite         bool
}

func (l *writeLoop) write(ctx context.Context, chunk []byte, chunkIndex uint16, lastWriteSize int) (int, error) {
	size := l.writeBlockSize
	if len(chunk) <= size {
		size = len(chunk)
	} else if l.noSplitWrite && lastWriteSize <= l.writeBlockSize {
		size = len(chunk) - lastWriteSize
	}
	if _, err := l.chunkstore.writeChunk(ctx, l.blobName, chunkIndex, chunk[:size]); err != nil {
		return 0, err
	}
	return size, nil
}

func (l *writeLoop) run(ctx context.Context) {
	flushTime := time.Now().Add(year)
	open := true
	chunk := []byte{}
	volatileTail := []byte{}
	chunkIndex := uint16(0)
	for open {
		timeout := false
		lastWriteSize := 0
		bytesFlushed := 0
		var req *WriteRequest

		delay := time.NewTimer(time.Until(flushTime))
		// don't leak the timer
		defer timeutil.StopAndDrainTimer(delay)
		// Get the write request for this iteration.
		select {
		case req, open = <-l.writeChannel:
			if req != nil {
				// We received a write request; update the context, chunk, and tail.
				ctx = req.ctx
				if req.Chunk == nil {
					// nil Write is a manual flush
					flushTime = time.Unix(0, 0)
				}
				if req.VolatileTail != nil {
					// nil Tail means don't overwrite the tail. This accompanies manual
					// flushes and no-op writes (to get updated chunkIndex from potential
					// automatic flushes).
					if string(req.VolatileTail) == string(volatileTail) {
						// We did not change the tail, update the request to reflect this.
						req.VolatileTail = nil
					} else {
						volatileTail = req.VolatileTail
					}
				}
				chunk = append(chunk, req.Chunk...)
				lastWriteSize = len(req.Chunk)
				if req.Close {
					open = false
				}
			}
		case <-delay.C:
			// We timed out waiting for a write request. Flush the contents of the
			// chunk to disk.
			timeout = true
		case <-ctx.Done():
			open = false

			// The context was canceled before the file was closed; extend the life of
			// the context so we can flush the contents of the chunk and tail to disk.
			var cancel func()
			ctx, cancel = background.ExtendContextForFinalization(ctx, time.Second*10)
			defer cancel()
		}

		if !open {
			// We are closing the file, so any tail must be written lest it be lost.
			chunk = append(chunk, volatileTail...)
			lastWriteSize = len(volatileTail)
			volatileTail = []byte{}
		}
		var err error
		for err == nil && len(chunk) >= l.writeBlockSize {
			// Write blocks until we have fewer than writeBlockSize bytes remaining
			// or we encounter a write error.
			var n int
			n, err = l.write(ctx, chunk, chunkIndex, lastWriteSize)
			if err == nil {
				bytesFlushed += n
				chunk = chunk[n:]
				chunkIndex++
			}
		}
		// We do not attempt to write if we have already encountered a write error
		// in this iteration of the loop, and instead we simply move on.
		// If we're closing the file and haven't written a chunk (chunkIndex == 0)
		// or there is data cached to write, we must write the chunk to disk.
		// Otherwise, if it is time to flush the file and there is data to write,
		// we must write the chunk to disk.
		// The iterative writes above guarantee, absent an error, that chunk will be
		// writeBlockSize or shorter.
		if err == nil {
			if !open && (chunkIndex == 0 || len(chunk) > 0) || time.Now().After(flushTime) && len(chunk) > 0 {
				var n int
				n, err = l.write(ctx, chunk, chunkIndex, lastWriteSize)
				if err == nil {
					bytesFlushed += n
					chunk = chunk[n:]
					chunkIndex++
				}
			}
		}
		if len(chunk) == 0 {
			// There is no data to write; we don't need to wake to flush data.
			flushTime = time.Now().Add(year)
		} else if time.Now().Add(l.writeTimeoutDuration).Before(flushTime) || bytesFlushed > 0 {
			// There is data to write, we need to wake after a timeout to flush data.
			// If flushTime is further in the future than writeTimeoutDuration, or we
			//just wrote data to disk, reset the flushTime.
			flushTime = time.Now().Add(l.writeTimeoutDuration)
		}
		// We report lastWriteSize as the number of bytes written because we may not
		// flush all the bytes, but they will still be buffered to be written after
		// we return. Thus, any and all write failures are instead reported in the
		// error object as opposed to reflected in the number of bytes written.
		result := &WriteResult{Size: lastWriteSize, Err: err, LastChunkIndex: chunkIndex - 1, Timeout: timeout, BytesFlushed: bytesFlushed, Close: !open}
		l.writeResultChannel <- result

		if l.writeHook != nil {
			if bytesFlushed != 0 || !open || !req.IsEmpty() {
				// Only call the writeHook if a state change has occurred.

				// All work has been done for this write; trigger the writeHook.
				l.writeHook(ctx, req, result, chunk, volatileTail)
			}
		}
	}
	close(l.writeResultChannel)
}

func (c *Chunkstore) Writer(ctx context.Context, blobName string, co *ChunkstoreWriterOptions) *ChunkstoreWriter {
	writeBlockSize := c.writeBlockSize
	if co != nil && co.WriteBlockSize != 0 {
		writeBlockSize = co.WriteBlockSize
	}
	writeTimeoutDuration := 5 * time.Second
	if co != nil && co.WriteTimeoutDuration != 0 {
		writeTimeoutDuration = co.WriteTimeoutDuration
	}
	var writeHook func(context.Context, *WriteRequest, *WriteResult, []byte, []byte)
	if co != nil {
		writeHook = co.WriteHook
	}
	noSplitWrite := false
	if co != nil {
		noSplitWrite = co.NoSplitWrite
	}

	writer := &ChunkstoreWriter{
		blobName:     blobName,
		writeChannel: make(chan *WriteRequest, 2),
		// writeResultChannel must be length 2 so that both a timeout event and a
		// close event could be simultaneously queued.
		writeResultChannel:   make(chan *WriteResult, 2),
		writeTimeoutDuration: writeTimeoutDuration,
	}

	loop := &writeLoop{
		chunkstore:           c,
		blobName:             blobName,
		writeChannel:         writer.writeChannel,
		writeResultChannel:   writer.writeResultChannel,
		writeHook:            writeHook,
		writeBlockSize:       writeBlockSize,
		writeTimeoutDuration: writeTimeoutDuration,
		noSplitWrite:         noSplitWrite,
	}

	go loop.run(ctx)

	return writer
}
