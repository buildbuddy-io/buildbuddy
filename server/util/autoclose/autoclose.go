package autoclose

import (
	"io"
)

// AutoCloser closes the provided ReadCloser upon the
// first call to Read which returns a non-nil error.
type autoCloser struct {
	io.ReadCloser
}

func (c *autoCloser) Read(data []byte) (int, error) {
	n, err := c.ReadCloser.Read(data)
	if err != nil {
		defer c.ReadCloser.Close()
	}
	return n, err
}

func (c *autoCloser) Close() error {
	return c.ReadCloser.Close()
}

func AutoCloser(readCloser io.ReadCloser) io.ReadCloser {
	return &autoCloser{readCloser}
}
