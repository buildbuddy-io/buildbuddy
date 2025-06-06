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

// TeeReadCacher returns a ReadCloser that behaves like a TeeReader:
// every byte read from the input ReadCloser is written to the cache.
//
// Cache writes are best-effort: if the TeeReadCacher encounters any errors writing to the cache,
// it will log it, attempt no more cache writes, but still allow reads.
//
// When the TeeReadCacher encounters EOF, it will call commit on the cache.
// The cache commit is best-effort: errors will be logged but otherwise ignored.
//
// Closing the TeeReadCacher will also close the given ReadCloser and CommittedWriteCloser.
func TeeReadCacher(rc io.ReadCloser, cache interfaces.CommittedWriteCloser) (io.ReadCloser, error) {
	if rc == nil {
		return nil, status.FailedPreconditionError("cannot create teeReadCacher from nil ReadCloser")
	}
	if cache == nil {
		return nil, status.FailedPreconditionError("cannot create teeReadCacher from nil cache")
	}
	return &teeReadCacher{
		rc:    rc,
		cache: cache,
	}, nil
}

type teeReadCacher struct {
	rc    io.ReadCloser
	cache interfaces.CommittedWriteCloser

	cacheErr  error
	committed bool
}

func (t *teeReadCacher) Read(p []byte) (int, error) {
	n, err := t.rc.Read(p)

	if t.committed {
		log.Warning("teeReadCacher already committed, cannot write to cache")
		return n, err
	}

	if t.cacheErr != nil {
		// Already encountered error writing, do not attempt to write or commit.
		return n, err
	}

	if n > 0 {
		written := 0
		written, t.cacheErr = t.cache.Write(p[:n])
		if t.cacheErr != nil {
			log.Warningf("teeReadCacher error writing to cache: %s", t.cacheErr)
			return n, err
		}
		if written < n {
			t.cacheErr = io.ErrShortWrite
		}
	}

	if err == io.EOF {
		t.committed = true
		if err := t.cache.Commit(); err != nil {
			log.Warningf("Error commiting to cache in teeReadCacher: %s", err)
			t.cacheErr = err
		}
	}

	return n, err
}

func (t *teeReadCacher) Close() error {
	err := t.rc.Close()
	if err := t.cache.Close(); err != nil {
		log.Warningf("Error closing cache writer in teeReadCacher: %s", err)
	}
	return err
}

// NewBestEffortWriter wraps the given Writer.
// If a write call to the given writer fails, subsequent writes will fail with an error.
// Calling Err() on the BestEffortWriter returns the first error encountered, if any.
func NewBestEffortWriter(w io.Writer) *BestEffortWriter {
	return &BestEffortWriter{w: w}
}

type BestEffortWriter struct {
	w   io.Writer
	err error
}

func (b *BestEffortWriter) Write(p []byte) (int, error) {
	if b.err != nil {
		return 0, status.FailedPreconditionErrorf("BestEffortWriter already encountered error, cannot receive writes: %s", b.err)
	}
	written, err := b.w.Write(p)
	if err != nil {
		b.err = err
	}
	return written, err
}

func (b *BestEffortWriter) Err() error {
	return b.err
}
