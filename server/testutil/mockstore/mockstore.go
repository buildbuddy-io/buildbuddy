package mockstore

import (
	"context"
	"fmt"
	"os"
	"time"
)

type Mocktext struct{}

func (m *Mocktext) Deadline() (time.Time, bool) {
	return time.Unix(0, 0), false
}

func (m *Mocktext) Done() <-chan struct{} {
	return nil
}

func (m *Mocktext) Err() error {
	return nil
}

func (m *Mocktext) Value(key interface{}) interface{} {
	return nil
}

type Mockstore struct {
	BlobMap map[string][]byte
}

func New() *Mockstore {
	return &Mockstore{BlobMap: make(map[string][]byte)}
}

func (m *Mockstore) BlobExists(_ context.Context, blobName string) (bool, error) {
	_, ok := m.BlobMap[blobName]
	return ok, nil
}
func (m *Mockstore) ReadBlob(_ context.Context, blobName string) ([]byte, error) {
	if value, ok := m.BlobMap[blobName]; !ok {
		return nil, fmt.Errorf("%s not present in mockstore map: %w", blobName, os.ErrNotExist)
	} else {
		return value, nil
	}
}
func (m *Mockstore) WriteBlob(_ context.Context, blobName string, data []byte) (int, error) {
	m.BlobMap[blobName] = make([]byte, len(data))
	return copy(m.BlobMap[blobName], data), nil
}
func (m *Mockstore) DeleteBlob(_ context.Context, blobName string) error {
	delete(m.BlobMap, blobName)
	return nil
}
