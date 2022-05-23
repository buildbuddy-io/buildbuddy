package mockstore

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

type Context struct{}

func (m *Context) Deadline() (time.Time, bool) {
	return time.Unix(0, 0), false
}

func (m *Context) Done() <-chan struct{} {
	return nil
}

func (m *Context) Err() error {
	return nil
}

func (m *Context) Value(key interface{}) interface{} {
	return nil
}

type Mockstore struct {
	mu      sync.Mutex
	BlobMap map[string][]byte
}

func New() *Mockstore {
	return &Mockstore{BlobMap: make(map[string][]byte)}
}

func (m *Mockstore) BlobExists(_ context.Context, blobName string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.BlobMap[blobName]
	return ok, nil
}
func (m *Mockstore) ReadBlob(_ context.Context, blobName string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if value, ok := m.BlobMap[blobName]; !ok {
		return nil, fmt.Errorf("%s not present in mockstore map: %w", blobName, os.ErrNotExist)
	} else {
		return value, nil
	}
}

func (m *Mockstore) Set(blobName string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.BlobMap[blobName] = data
}

func (m *Mockstore) GetBlobMap() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := make(map[string][]byte, len(m.BlobMap))
	for k, v := range m.BlobMap {
		r[k] = v
	}
	return r
}

func (m *Mockstore) WriteBlob(_ context.Context, blobName string, data []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.BlobMap[blobName] = make([]byte, len(data))
	return copy(m.BlobMap[blobName], data), nil
}
func (m *Mockstore) DeleteBlob(_ context.Context, blobName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.BlobMap, blobName)
	return nil
}
