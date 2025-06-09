package ioutil

import (
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
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

	committed bool
}

func (b *BestEffortWriter) Write(p []byte) (int, error) {
	if b.err != nil {
		return len(p), nil
	}
	written, err := b.w.Write(p)
	if err != nil {
		b.err = err
	}
	if written < len(p) {
		b.err = io.ErrShortWrite
	}
	if b.err != nil {
		log.Warningf("BestEffortWriter write error: %s", b.err)
	}
	return len(p), nil
}

func (b *BestEffortWriter) Commit() error {
	if b.committed {
		return nil
	}
	if b.err != nil {
		return nil
	}
	b.committed = true
	if committer, ok := b.w.(interfaces.Committer); ok {
		if err := committer.Commit(); err != nil {
			b.err = err
			log.Warningf("Error committing BestEffortWriter: %s", err)
		}
	}
	return nil
}

func (b *BestEffortWriter) Close() error {
	if closer, ok := b.w.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			log.Warningf("Error closing BestEffortWriter: %s", err)
		}
	}
	return nil
}

func (b *BestEffortWriter) Err() error {
	return b.err
}

// BestEffortReader returns a ReadCloser that behaves like an io.TeeReader:
// each read from the given reader will be written to the given writer.
// Any write errors will be ignored. BestEffortTeeReader will not make any
// writes after encountering the first write error.
//
// If the given writer is also a Committer, BestEffortTeeReader will call Commit
// upon receiving EOF from the given reader.
func BestEffortTeeReader(r io.Reader, w io.Writer) (io.ReadCloser, error) {
	if r == nil {
		return nil, status.FailedPreconditionError("Cannot construct BestEffortTeeReader with nil reader")
	}
	if w == nil {
		return nil, status.FailedPreconditionError("Cannot construct BestEffortTeeReader with nil writer")
	}
	return &bestEffortTeeReader{
		r: r,
		w: NewBestEffortWriter(w),
	}, nil
}

type bestEffortTeeReader struct {
	r io.Reader
	w *BestEffortWriter
}

func (t *bestEffortTeeReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if t.w.Err() != nil {
		return n, err
	}

	if n > 0 {
		t.w.Write(p[:n])
		if t.w.Err() != nil {
			return n, err
		}
	}

	if err == io.EOF {
		t.w.Commit()
	}

	return n, err

}

// Close closes both the underlying reader and writer (if they are Closers).
// Errors closing the writer will be logged but not returned.
func (t *bestEffortTeeReader) Close() error {
	var err error
	if closer, ok := t.r.(io.Closer); ok {
		err = closer.Close()
	}
	t.w.Close()
	return err
}
