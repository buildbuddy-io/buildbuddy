package blobstore

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

// A Blob must implement the io.Reader & io.Writer interfaces.
type Blob interface {
	Read(p []byte) (n int, err error)
	Write(p []byte) (n int, err error)
}

// A Blobstore must allow for getting and deleting blobs.
// Blobs with no content are not guaranteed to exist -- it's up to the
// implementation.
type Blobstore interface {
	GetBlob(blobName string) (Blob, error)
	DeleteBlob(blobName string) error
}

type DiskBlobStore struct {
	rootDir string
}

func NewDiskBlobStore(rootDir string) *DiskBlobStore {
	return &DiskBlobStore{
		rootDir: rootDir,
	}
}

func (d *DiskBlobStore) GetBlob(blobName string) (Blob, error) {
	// Probably could support nesting in directories here but we are lazy.
	if filepath.Base(blobName) != blobName {
		return nil, fmt.Errorf("blobName (%s) must not contain dirs.", blobName)
	}
	f, err := os.OpenFile(filepath.Join(d.rootDir, blobName), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return bufio.NewReadWriter(bufio.NewReader(f), bufio.NewWriter(f)), nil
}

func (d *DiskBlobStore) DeleteBlob(blobName string) error {
	if filepath.Base(blobName) != blobName {
		return fmt.Errorf("blobName (%s) must not contain dirs.", blobName)
	}
	return os.Remove(filepath.Join(d.rootDir, blobName))
}
