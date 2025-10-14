package lockingbuffer

import (
	"bytes"
	"io"
	"sync"
)

// LockingBuffer is a thread-safe buffer.
type LockingBuffer struct {
	buffer  bytes.Buffer
	mu      sync.RWMutex
	maxSize uint64 // 0 means unlimited
}

func New() *LockingBuffer {
	return &LockingBuffer{}
}

func NewWithMaxSize(maxSize uint64) *LockingBuffer {
	return &LockingBuffer{maxSize: maxSize}
}

func (lb *LockingBuffer) Len() int {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.buffer.Len()
}

func (lb *LockingBuffer) Write(p []byte) (int, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// If no size limit, behave as before
	if lb.maxSize == 0 {
		return lb.buffer.Write(p)
	}

	// Write the new data first
	n, err := lb.buffer.Write(p)
	if err != nil {
		return n, err
	}

	// If we've exceeded the limit, truncate from the beginning
	if uint64(lb.buffer.Len()) > lb.maxSize {
		data := lb.buffer.Bytes()
		keep := data[len(data)-int(lb.maxSize):]
		lb.buffer.Reset()
		lb.buffer.Write(keep)
	}

	return n, nil
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

func (lb *LockingBuffer) String() string {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.buffer.String()
}
