package blockio

import (
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// Store models a block-level storage system, which is useful as a backend for a
// Network Block Device (NBD) or userfaultfd(2).
type Store interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	// Sync flushes any buffered pages to the store, if applicable.
	Sync() error

	// Size returns the total addressable size of the store in bytes.
	SizeBytes() (int64, error)
}

// File implements the Store interface using standard file operations.
type File struct{ *os.File }

// NewFile returns a File store for an existing file.
func NewFile(path string) (*File, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return &File{File: f}, nil
}

func (f *File) SizeBytes() (int64, error) {
	return fileSizeBytes(f.File)
}

// Mmap implements the Store interface using a memory-mapped file.
type Mmap struct {
	file *os.File
	data []byte
}

// NewMmap returns an Mmap for an existing file.
func NewMmap(path string) (*Mmap, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	s, err := f.Stat()
	if err != nil {
		return nil, err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(s.Size()), syscall.PROT_WRITE, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("mmap: %s", err)
	}
	return &Mmap{file: f, data: data}, nil
}

func (m *Mmap) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || int(off)+len(p) > len(m.data) {
		return 0, status.InvalidArgumentErrorf("invalid offset %x", off)
	}
	copy(p, m.data[int(off):int(off)+len(p)])
	return len(p), nil
}

func (m *Mmap) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 || int(off)+len(p) > len(m.data) {
		return 0, status.InvalidArgumentErrorf("invalid offset %x", off)
	}
	copy(m.data[int(off):int(off)+len(p)], p)
	return len(p), nil
}

func (m *Mmap) Sync() error {
	return m.file.Sync()
}

func (m *Mmap) Close() error {
	if err := syscall.Munmap(m.data); err != nil {
		_ = m.file.Close()
		return err
	}
	return m.file.Close()
}

func (m *Mmap) SizeBytes() (int64, error) {
	return fileSizeBytes(m.file)
}

func fileSizeBytes(f *os.File) (int64, error) {
	s, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return s.Size(), nil
}

// TODO: add a Store supporting copy-on-write.
