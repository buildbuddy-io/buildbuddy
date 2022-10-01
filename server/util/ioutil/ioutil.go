package ioutil

import (
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
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
	log.Printf("CustomCommitWriteCloser: Write %d", len(buf))
	n, err := c.w.Write(buf)
	c.bytesWritten += int64(n)
	return n, err
}

func (c *CustomCommitWriteCloser) Commit() error {
	log.Printf("CustomCommitWriteCloser: Commit")
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
	log.Printf("CustomCommitWriteCloser: Close")
	var firstErr error

	// Close may free resources, so all Close functions should be called.
	// The first error encountered will be returned.
	if closer, ok := c.w.(io.Closer); ok {
		if err := closer.Close(); err != nil && firstErr == nil {
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

func NewCustomCommitWriteCloser(w io.Writer) *CustomCommitWriteCloser {
	log.Printf("NewCustomCommitWriteCloser: %+v", w)
	return &CustomCommitWriteCloser{
		w: w,
	}
}

type CloseCommitReplacer struct {
	io.WriteCloser
	committed bool
}

func (r *CloseCommitReplacer) Commit() error {
	defer func() {
		r.committed = true
	}()
	return r.WriteCloser.Close()
}

func (r *CloseCommitReplacer) Close() error {
	return nil
}

func AutoUpgradeCloser(wc io.WriteCloser) interfaces.CommittedWriteCloser {
	if cwc, ok := wc.(interfaces.CommittedWriteCloser); ok {
		return cwc
	}

	return &CloseCommitReplacer{
		WriteCloser: wc,
	}
}
