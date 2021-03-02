package locking_buffer

import (
	"bytes"
	"sync"
)

// LockingBuffer is a thread-safe buffer.
type LockingBuffer struct {
	buffer bytes.Buffer
	mu     sync.RWMutex
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
