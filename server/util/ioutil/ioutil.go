package ioutil

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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

type CloseFunc func() error
type CommitFunc func(int64) error

type CustomCommitWriteCloser struct {
	w            io.Writer
	bytesWritten int64
	committed    bool

	CloseFn  CloseFunc
	CommitFn CommitFunc
}

func (c *CustomCommitWriteCloser) Write(buf []byte) (int, error) {
	n, err := c.w.Write(buf)
	c.bytesWritten += int64(n)
	return n, err
}

func (c *CustomCommitWriteCloser) Commit() error {
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

func (c *CustomCommitWriteCloser) Close() error {
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

// NewCustomCommitWriteCloser wraps an io.Writer/interfaces.CommittedWriteCloser
// and returns a pointer to a CustomCommitWriteCloser, which implements
// interfaces.CommittedWriteCloser but allows adding on custom logic that will
// be called when Commit or Close methods are called.
func NewCustomCommitWriteCloser(w io.Writer) *CustomCommitWriteCloser {
	return &CustomCommitWriteCloser{
		w: w,
	}
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

// LineWriter buffers data until a newline byte is observed, then forwards only
// complete lines to the underlying writer. Partial lines remain buffered until
// completed.
type LineWriter struct {
	w   io.Writer
	buf []byte
	err error
}

func NewLineWriter(w io.Writer) *LineWriter {
	return &LineWriter{w: w}
}

func (lw *LineWriter) Write(p []byte) (int, error) {
	if lw.err != nil {
		return 0, lw.err
	}
	total := 0
	for len(p) > 0 {
		idx := bytes.IndexByte(p, '\n')
		if idx == -1 {
			lw.buf = append(lw.buf, p...)
			total += len(p)
			break
		}

		segment := p[:idx+1]
		lw.buf = append(lw.buf, segment...)
		total += len(segment)

		if err := lw.flushBuffered(); err != nil {
			// total reflects bytes accepted into the buffer before the error.
			return total, err
		}

		p = p[idx+1:]
	}
	return total, nil
}

// Flush writes any buffered data to the underlying writer, including
// partial lines that don't end with a newline.
func (lw *LineWriter) Flush() error {
	if lw.err != nil {
		return lw.err
	}
	return lw.flushBuffered()
}

func (lw *LineWriter) flushBuffered() error {
	if lw.err != nil {
		return lw.err
	}
	for len(lw.buf) > 0 {
		n, err := lw.w.Write(lw.buf)
		if n > 0 {
			lw.buf = lw.buf[n:]
		}
		if err != nil {
			lw.err = err
			return err
		}
		if n == 0 {
			lw.err = io.ErrShortWrite
			return lw.err
		}
	}
	return nil
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
