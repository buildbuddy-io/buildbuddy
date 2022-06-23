package lockingbuffer

import (
	"bytes"
	"io"
	"sync"
)

// LockingBuffer is a thread-safe buffer.
type LockingBuffer struct {
	buffer bytes.Buffer
	mu     sync.RWMutex
}

func New() *LockingBuffer {
	return &LockingBuffer{}
}

func (lb *LockingBuffer) Len() int {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.buffer.Len()
}

func (lb *LockingBuffer) Write(p []byte) (int, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.buffer.Write(p)
}

func (lb *LockingBuffer) Read(p []byte) (int, error) {
	// NOTE: Can't use RLock here since the impl of buffer.Read
	// mutates the buffer's read cursor.
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.buffer.Read(p)
}

func (lb *LockingBuffer) Reset() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.buffer.Reset()
}

// ReadAll provides a thread-safe alternative to io.ReadAll for lockingbuffer.
func (lb *LockingBuffer) ReadAll() ([]byte, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	b, err := io.ReadAll(bytes.NewReader(lb.buffer.Bytes()))
	lb.buffer.Reset()
	return b, err
}
