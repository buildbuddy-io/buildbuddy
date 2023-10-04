// flock provides a slightly more idiomatic wrapper around flock(2).
package flock

import (
	"os"
	"syscall"
)

// Flock implements advisory locking using a file.
type Flock struct{ *os.File }

// Open opens an existing file, returning a Flock instance.
// The caller is responsible for closing the returned file, which will release
// any held locks.
func Open(path string) (*Flock, error) {
	f, err := os.OpenFile(path, 0, 0)
	if err != nil {
		return nil, err
	}
	return &Flock{File: f}, nil
}

// Create creates a file, returning a Flock instance.
// The caller is responsible for closing the returned file, which will release
// any held locks.
func Create(path string) (*Flock, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Flock{File: f}, nil
}

// Lock locks with LOCK_EX. See flock(2) for more details.
//
// NOTE: this doesn't work the same way as a sync.Mutex Lock, in that the lock
// can be acquired more than once for the same file descriptor.
func (f *Flock) Lock() error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
}

// TryLock locks with LOCK_EX and LOCK_NB. See flock(2) for more details.
func (f *Flock) TryLock() error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// Unlock locks with LOCK_UN. See flock(2) for more details.
func (f *Flock) Unlock() error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
