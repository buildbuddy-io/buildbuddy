package nbdio

import (
	"io"
	"os"
)

// BlockDevice implements the required operations for a hosted network block
// device.
type BlockDevice interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
	Close() error
	Size() (int64, error)
}

// File implements the BlockDevice interface using an *os.File.
type File struct {
	*os.File
}

func (f *File) Size() (int64, error) {
	s, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return s.Size(), nil
}

// TODO: OverlayDevice
