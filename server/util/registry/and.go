package registry

import (
	"io"
)

// AndReadCloser implements io.ReadCloser by reading from a particular io.Reader
// and then calling the provided "Close()" method.
type AndReadCloser struct {
	io.Reader
	CloseFunc func() error
}

var _ io.ReadCloser = (*AndReadCloser)(nil)

// Close implements io.ReadCloser
func (rac *AndReadCloser) Close() error {
	return rac.CloseFunc()
}

// WriteCloser implements io.WriteCloser by reading from a particular io.Writer
// and then calling the provided "Close()" method.
type WriteCloser struct {
	io.Writer
	CloseFunc func() error
}

var _ io.WriteCloser = (*WriteCloser)(nil)

// Close implements io.WriteCloser
func (wac *WriteCloser) Close() error {
	return wac.CloseFunc()
}
