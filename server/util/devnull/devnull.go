package devnull

import (
	"io"
)

// A writer that drops anything written to it.
// Useful when you need an io.Writer but don't intend
// to actually write bytes to it.
type discardWriteCloser struct {
	io.Writer
}

// NewWriteCloser returns an io.WriteCloser that wraps ioutil.Discard,
// dropping any bytes written to it and returning nil on Close.
func NewWriteCloser() io.WriteCloser {
	return &discardWriteCloser{
		io.Discard,
	}
}

func (discardWriteCloser) Close() error {
	return nil
}
