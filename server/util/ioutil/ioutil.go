package ioutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	// spillWriterBufSizeBytes is the size of the pooled bufio.Writer buffers
	// used for writing to spill files.
	spillWriterBufSizeBytes = 4096
)

var (
	// ErrLimitExceeded is returned when a read exceeds its configured size limit.
	ErrLimitExceeded = errors.New("read limit exceeded")

	// spillBufferPool recycles the in-memory buffers held by SpillBuffers,
	// since callers like the executor create a couple of them per task.
	// Requests larger than the pool max are capped, and appends past the
	// pooled capacity fall back to regular allocation.
	spillBufferPool = bytebufferpool.VariableSize(1024 * 1024)

	// spillWriterPool recycles the bufio.Writers which buffer writes to spill
	// files, so that each spill doesn't allocate a fresh write buffer.
	spillWriterPool = bytebufferpool.NewVariableWriteBufPool(spillWriterBufSizeBytes)
)

// A writer that drops anything written to it.
// Useful when you need an io.Writer but don't intend
// to actually write bytes to it.
type discardWriteCloser struct {
	io.Writer
}

// DiscardWriteCloser returns an io.WriteCloser that wraps ioutil.Discard,
// dropping any bytes written to it and returning nil on Close.
func DiscardWriteCloser() *discardWriteCloser {
	return &discardWriteCloser{
		io.Discard,
	}
}

func (discardWriteCloser) Commit() error {
	return nil
}
func (discardWriteCloser) Close() error {
	return nil
}

type readCloser struct {
	io.Reader
	io.Closer
}

// LimitReadCloser returns a readCloser with a LimitReader.
func LimitReadCloser(reader io.ReadCloser, limit int64) io.ReadCloser {
	return &readCloser{
		io.LimitReader(reader, limit),
		reader,
	}
}

type CloseFunc func() error
type CommitFunc func(int64) error

type CustomCommitWriteCloser interface {
	interfaces.CommittedWriteCloser

	SetCloseFn(CloseFunc)
	SetCommitFn(CommitFunc)
}

type customCommitWriteCloser struct {
	w            io.Writer
	bytesWritten int64
	committed    bool

	CloseFn  CloseFunc
	CommitFn CommitFunc
}

func (c *customCommitWriteCloser) Write(buf []byte) (int, error) {
	n, err := c.w.Write(buf)
	c.bytesWritten += int64(n)
	return n, err
}

func (c *customCommitWriteCloser) Commit() error {
	if c.committed {
		return status.FailedPreconditionError("CommitWriteCloser already committed, cannot commit again")
	}

	// Commit functions are run in order. If a commit function at a lower
	// level succeeds, the one above it should succeed as well.
	//
	// For example, take a writer first commits a file to the file system
	// and then writes some metadata to a database. If the file system
	// write succeeds, then the db write should succeed as well. In the case
	// where the filesystem write fails, the db write will not be executed.
	defer func() {
		c.committed = true
	}()

	if committer, ok := c.w.(interfaces.Committer); ok {
		if err := committer.Commit(); err != nil {
			return err
		}
	}

	if c.CommitFn != nil {
		return c.CommitFn(c.bytesWritten)
	}
	return nil
}

func (c *customCommitWriteCloser) Close() error {
	var firstErr error

	// Close may free resources, so all Close functions should be called.
	// The first error encountered will be returned.
	if closer, ok := c.w.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			firstErr = err
		}
	}

	if c.CloseFn != nil {
		if err := c.CloseFn(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *customCommitWriteCloser) SetCloseFn(fn CloseFunc) {
	c.CloseFn = fn
}

func (c *customCommitWriteCloser) SetCommitFn(fn CommitFunc) {
	c.CommitFn = fn
}

type customCommitWriteSeekCloser struct {
	*customCommitWriteCloser
	io.Seeker
}

// NewCustomCommitWriteCloser wraps an io.Writer/interfaces.CommittedWriteCloser
// and returns a customCommitWriteCloser, which implements
// interfaces.CommittedWriteCloser but allows adding on custom logic that will
// be called when Commit or Close methods are called. If the wrapped writer
// implements io.Seeker, the returned value will also implement io.Seeker.
func NewCustomCommitWriteCloser(w io.Writer) CustomCommitWriteCloser {
	cwc := &customCommitWriteCloser{
		w: w,
	}
	if seeker, ok := w.(io.Seeker); ok {
		return &customCommitWriteSeekCloser{
			customCommitWriteCloser: cwc,
			Seeker:                  seeker,
		}
	}
	return cwc
}

// Counter keeps a count of all bytes written, discarding any written bytes.
// It is not safe for concurrent use.
type Counter struct{ n int64 }

func (c *Counter) Write(p []byte) (n int, err error) {
	c.n += int64(len(p))
	return len(p), nil
}

// Count returns the total number of bytes written.
func (c *Counter) Count() int64 {
	return c.n
}

// ReadAllLimited reads from r until EOF or until the read exceeds limit.
// It returns ErrLimitExceeded if more than limit bytes would be read.
func ReadAllLimited(r io.Reader, limit int64) ([]byte, error) {
	const maxInt64 = int64(^uint64(0) >> 1)
	if limit == maxInt64 {
		return io.ReadAll(r)
	}
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, ErrLimitExceeded
	}
	return b, nil
}

// ReadTryFillBuffer tries to fill the given buffer by repeatedly reading
// from the reader until it runs out of data. If the underlying reader does
// not have enough data left to fill the buffer, the returned buffer will only
// be partially filled.
func ReadTryFillBuffer(r io.Reader, buf []byte) (int, error) {
	n, err := io.ReadFull(r, buf)
	if err == io.ErrUnexpectedEOF {
		return n, nil
	}
	return n, err
}

type byteRepeater byte

func (r byteRepeater) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r)
	}
	return len(p), nil
}

// NewByteRepeater returns an io.Reader that endlessly repeats b.
func NewByteRepeater(b byte) io.Reader {
	return byteRepeater(b)
}

func NewBestEffortWriter(w io.Writer) *BestEffortWriter {
	return &BestEffortWriter{w: w}
}

// BestEffortWriter wraps a Writer.
// Calls to Write will always succeed.
// If a write call to the wrapped writer fails, the BestEffortWriter will not make any more write calls on the wrapper writer.
// Calling Err() on the BestEffortWriter returns the first error encountered, if any.
type BestEffortWriter struct {
	w   io.Writer
	err error
}

func (b *BestEffortWriter) Write(p []byte) (int, error) {
	if b.err != nil {
		return len(p), nil
	}
	_, err := b.w.Write(p)
	if err != nil {
		b.err = err
	}
	return len(p), nil
}

func (b *BestEffortWriter) Err() error {
	return b.err
}

// DoubleBufferWrite is a buffered writer inspired by graphics double buffering,
// but with a few differences.
//   - It buffers writes and flushes them in the background as soon as any
//     previous write has completed, allowing both the reader and writer in a
//     pipelined IO operation to make steady progress.
//   - The buffer starts at one size but can grow up to a limit.
//   - Technically, it can be using 3 buffers at a time: one for an inflight
//     write, one full one that's waiting to be written out, and one that's
//     being filled. It can also just be using one, if the outgoing writer is
//     faster than the incoming writes.
type DoubleBufferWriter struct {
	ctx            context.Context
	w              interfaces.CommittedWriteCloser
	minimumBufSize int
	maximumBufSize int
	bufPool        *bytebufferpool.VariableSizePool
	writes         chan []byte
	errors         chan error

	bufferHasSpace bool
	lastErr        error
	closedWrites   bool
	writeCount     int
}

// NewDoubleBufferWriter creates a new DoubleBufferWriter that writes to w.
// `bufPool` should be able to allocate buffers up to `maximumBufSize`, or this
// may allocate a lot.
func NewDoubleBufferWriter(ctx context.Context, w interfaces.CommittedWriteCloser, bufPool *bytebufferpool.VariableSizePool, minimumBufSize, maximumBufSize int) *DoubleBufferWriter {
	d := &DoubleBufferWriter{
		ctx:            ctx,
		w:              w,
		minimumBufSize: minimumBufSize,
		maximumBufSize: maximumBufSize,
		bufPool:        bufPool,
		writes:         make(chan []byte, 1),
		errors:         make(chan error, 1),
	}
	go d.runWriter()
	return d
}

func (w *DoubleBufferWriter) runWriter() {
	defer close(w.errors)
	for data := range w.writes {
		n, err := w.w.Write(data)

		dataLen := len(data)
		// Instead of passing buffers back to the goroutine calling Write(),
		// just use the pool to manage buffer reuse. This is simpler, just as
		// fast, and returns buffers to the pool sooner when there are no more
		// writes.
		w.bufPool.Put(data)
		if err == nil && n < dataLen {
			err = io.ErrShortWrite
		}
		if err != nil {
			w.errors <- err
			return
		}
	}
}

// If the estimate for the minimum buffer size is smaller than the incoming
// write, this may get a bigger buffer to avoid many small outgoing writes for a
// single incoming write. This will also get a new buffer if we don't have one
// already.
func (w *DoubleBufferWriter) resizeBuffer(buffer []byte, chunkSize int) []byte {
	if cap(buffer) >= w.maximumBufSize {
		return buffer
	}
	w.minimumBufSize = min(max(chunkSize, w.minimumBufSize), w.maximumBufSize)
	if w.minimumBufSize <= cap(buffer) {
		return buffer
	}
	newBuf := w.bufPool.Get(int64(w.minimumBufSize))
	if len(buffer) > 0 {
		copy(newBuf, buffer)
	}
	w.bufPool.Put(buffer)
	return newBuf[:len(buffer)]
}

func (w *DoubleBufferWriter) Write(data []byte) (int, error) {
	if w.closedWrites {
		return 0, errors.New("DoubleBufferWriter tried to write after Close or Commit")
	}
	if w.lastErr != nil {
		return 0, w.lastErr
	}
	initialDataSize := len(data)
	for len(data) > 0 {
		var buffer []byte
		if w.bufferHasSpace {
			select {
			case buffer = <-w.writes:
			default:
			}
		}
		buffer = w.resizeBuffer(buffer, initialDataSize)

		// Append as much data as can fit from data into buffer.
		prevBufLen := len(buffer)
		// Copy into a reslice of buffer that has len == cap.
		copied := copy(buffer[prevBufLen:cap(buffer)], data)
		// Increase the len of buffer.
		buffer = buffer[:prevBufLen+copied]
		data = data[copied:]

		select {
		case <-w.ctx.Done():
			w.lastErr = w.ctx.Err()
		case err := <-w.errors:
			w.lastErr = err
		case w.writes <- buffer:
			w.bufferHasSpace = len(buffer) < cap(buffer)
		}
		if w.lastErr != nil {
			return initialDataSize - (copied + len(data)), w.lastErr
		}
	}
	return initialDataSize, nil
}

func (w *DoubleBufferWriter) closeWrites() {
	if w.closedWrites {
		return
	}
	w.closedWrites = true
	close(w.writes)
	for err := range w.errors {
		if w.lastErr == nil {
			w.lastErr = err
		}
	}
}

// Commit flushes the writes and then commits to the underlying writer. It
// returns an error if either of those fails.
func (w *DoubleBufferWriter) Commit() error {
	w.closeWrites()
	if w.lastErr != nil {
		return w.lastErr
	}
	return w.w.Commit()
}

// Close will return any outstanding buffers to the pool and return the result
// of closing the underlying writer.
func (w *DoubleBufferWriter) Close() error {
	w.closeWrites()
	for buf := range w.writes {
		w.bufPool.Put(buf)
	}
	return w.w.Close()
}

// PreserveNewlinesSplitFunc is a [bufio.SplitFunc] that can be used with
// [bufio.Scanner]. It keeps the newlines at the end if present, so that
// concatenating the scanned Text() reproduces the original input exactly. By
// contrast, the default split function splits on newlines but discards all
// trailing newline characters, which can be undesired.
func PreserveNewlinesSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	// Search for newline characters
	for i := range data {
		if data[i] == '\n' {
			// Include up to and including the '\n'
			return i + 1, data[0 : i+1], nil
		}
	}
	// If at EOF, return the remaining data (may not have newline)
	if atEOF {
		return len(data), data, nil
	}
	// Request more data
	return 0, nil, nil
}

type MultiReadCloser struct {
	io.Reader
	closers []io.Closer
}

// NewMultiReadCloser returns a new io.ReadCloser that reads from the
// readers in order. It closes all of the readers in order when
// Close is called.
func NewMultiReadCloser(rcs ...io.ReadCloser) io.ReadCloser {
	rs := make([]io.Reader, len(rcs))
	cs := make([]io.Closer, len(rcs))
	for i, rc := range rcs {
		rs[i] = rc
		cs[i] = rc
	}
	return &MultiReadCloser{
		Reader:  io.MultiReader(rs...),
		closers: cs,
	}
}

func (m *MultiReadCloser) Close() error {
	errs := make([]error, len(m.closers))
	for i, c := range m.closers {
		errs[i] = c.Close()
	}
	return errors.Join(errs...)
}

// SpillBuffer is a writer that buffers data in memory up to a fixed limit,
// then spills all data to a file once the limit is exceeded. This avoids any
// file IO in the common case where the total amount of data written is small.
// The in-memory buffers are recycled via a shared pool.
// It is not safe for concurrent use.
type SpillBuffer struct {
	path          string
	memLimitBytes int64
	buf           []byte
	closed        bool
	// reading is set once Reader is called, after which writes are rejected.
	// This makes misuse loud, since a write after the shared reader has
	// seeked the file would land at the wrong offset.
	reading bool
	file    *os.File
	// w buffers writes to the spill file, so that producers which write many
	// small chunks don't cost a write syscall per chunk.
	w *bytebufferpool.BufioWriter
}

// NewSpillBuffer returns a SpillBuffer which spills to a file created at the
// given path once more than memoryLimitBytes bytes are written. The file is
// created lazily, so nothing is written to disk if the limit is never
// exceeded. The caller is responsible for calling Close once the contents
// are no longer needed.
func NewSpillBuffer(path string, memoryLimitBytes int64) *SpillBuffer {
	return &SpillBuffer{path: path, memLimitBytes: memoryLimitBytes}
}

func (b *SpillBuffer) Write(p []byte) (int, error) {
	if b.closed || b.reading {
		return 0, errors.New("SpillBuffer is not writable after Reader or Close")
	}
	if b.file == nil {
		if int64(len(b.buf))+int64(len(p)) <= b.memLimitBytes {
			if b.buf == nil {
				// Take a pooled buffer on the first write rather than at
				// construction time, so that commands which produce no output
				// don't check out a buffer at all. Appends past the pooled
				// capacity fall back to regular allocation, which can happen
				// if the memory limit exceeds the pool's max buffer size.
				b.buf = spillBufferPool.Get(b.memLimitBytes)[:0]
			}
			b.buf = append(b.buf, p...)
			return len(p), nil
		}
		// This write puts us over the memory limit. Move the buffered data to
		// the spill file, then write to the file from here on.
		f, err := os.Create(b.path)
		if err != nil {
			return 0, fmt.Errorf("create spill file: %w", err)
		}
		if _, err := f.Write(b.buf); err != nil {
			// Leave the buffered data intact so that a subsequent write can
			// retry the spill, and remove the partial file so that a spill
			// file only exists on disk once the spill has fully succeeded.
			f.Close()
			os.Remove(b.path)
			return 0, fmt.Errorf("write buffered data to spill file: %w", err)
		}
		b.file = f
		b.w = spillWriterPool.Get(spillWriterBufSizeBytes)
		b.w.Reset(f)
		b.releaseBuf()
	}
	return b.w.Write(p)
}

// Reader returns a reader over all data written so far, positioned at the
// start. Writing must be complete before calling Reader, and Write calls
// fail once Reader has been called. The returned reader is a shared
// instance with a single seek cursor, so it must not be read from
// concurrently, and must not be read from after the SpillBuffer is closed.
func (b *SpillBuffer) Reader() (io.ReadSeeker, error) {
	if b.closed {
		return nil, errors.New("SpillBuffer readers must be obtained before Close")
	}
	b.reading = true
	if b.file == nil {
		return bytes.NewReader(b.buf), nil
	}
	if b.w != nil {
		if err := b.w.Flush(); err != nil {
			return nil, fmt.Errorf("flush spill file: %w", err)
		}
		// Writes are no longer allowed, so return the write buffer to the
		// pool now instead of holding it until Close.
		b.releaseWriter()
	}
	if _, err := b.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek spill file: %w", err)
	}
	return b.file, nil
}

// Close releases the in-memory buffer, and closes and removes the spill file
// if one was created. Close is safe to call more than once.
func (b *SpillBuffer) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	b.releaseBuf()
	if b.file == nil {
		return nil
	}
	// Skip flushing buffered writes, since the file is being removed anyway.
	closeErr := b.file.Close()
	b.file = nil
	b.releaseWriter()
	return errors.Join(closeErr, os.Remove(b.path))
}

// releaseWriter returns the pooled write buffer. Reset drops the reference
// to the file and clears any buffered data and error state left over from a
// failed flush.
func (b *SpillBuffer) releaseWriter() {
	if b.w == nil {
		return
	}
	b.w.Reset(io.Discard)
	spillWriterPool.Put(b.w)
	b.w = nil
}

// releaseBuf returns the in-memory buffer to the pool. After this, the
// buffer contents must not be read again, since the pool may hand the buffer
// out to another SpillBuffer.
func (b *SpillBuffer) releaseBuf() {
	if b.buf == nil {
		return
	}
	spillBufferPool.Put(b.buf)
	b.buf = nil
}
