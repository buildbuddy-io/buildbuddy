package lockmap

import (
	"sync"
	"time"
)

type refCountedMutex struct {
	sync.RWMutex
	collected bool
}

// Locker implements a per-key mutex. This can be useful when protecting access
// to many sparsely distributed and rarely conflicting resources, like rows in
// a database. A Locker interface is used like a generator for sync.RWMutexes:
//
// Ex.
//
//	l := lockmap.New()          <- this returns a Locker
//	unlockFn := l.Lock("key-1") <- this is like Lock()ing a RWMutex
//	defer unlockFn()            <- this is like Unlock()ing a RWMutex
//	// do work here.
//
// Ex 2.
//
//	l := lockmap.New()          <- this returns a Locker
//	unlockFn := l.Lock("key-1") <- this is like RLock()ing a RWMutex
//	defer unlockFn()            <- this is like RUnlock()ing a RWMutex
//	// do work here.
//
// Locks are uniquely identified by string, so the same resource can be both
// RLock()ed and Lock()ed by different threads, they will share the same
// RWMutex. The lockmap will cull unused locks periodically to save memory.
type Locker interface {
	Lock(key string) func()
	RLock(key string) func()
	Close() error
}

type perKeyMutex struct {
	mutexes sync.Map
	closed  chan struct{}
}

// New returns a new lock map.
// The caller must call Close() when the map is no longer needed.
func New() *perKeyMutex {
	pkm := &perKeyMutex{closed: make(chan struct{})}
	go pkm.gc()
	return pkm
}

func (p *perKeyMutex) gc() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-p.closed:
			return
		case <-t.C:
		}
		p.mutexes.Range(func(key, value any) bool {
			rcm := value.(*refCountedMutex)
			if !rcm.TryLock() {
				// The mutex is in use and thus shouldn't be collected.
				return true
			}
			// At this point we hold the exclusive lock, so there are two cases:
			// * There were no other goroutines waiting to acquire a lock, which
			//   means that the mutex should be collected.
			// * There were other goroutines waiting to acquire a lock, but they
			//   failed to acquire it in the short period of time between it
			//   becoming available and us acquiring it. As GC only rarely runs,
			//   this case is expected to be very uncommon. All waiting
			//   goroutines will need to refetch from the map.
			//
			// Mark the mutex as unusable before removing it from the map so
			// that there are never two active mutexes for the same key.
			rcm.collected = true
			rcm.Unlock()
			p.mutexes.Delete(key)
			return true
		})
	}
}

func (p *perKeyMutex) Lock(key string) func() {
	var rcm *refCountedMutex
	for {
		val, _ := p.mutexes.LoadOrStore(key, &refCountedMutex{})
		rcm = val.(*refCountedMutex)
		rcm.Lock()
		if !rcm.collected {
			break
		}
		// We hit the window between rcm.Unlock() and p.mutexes.Delete(key) in
		// the gc() method. Busy wait for the map slot to be cleared.
		rcm.Unlock()
	}
	return func() { rcm.Unlock() }
}

func (p *perKeyMutex) RLock(key string) func() {
	var rcm *refCountedMutex
	for {
		val, _ := p.mutexes.LoadOrStore(key, &refCountedMutex{})
		rcm = val.(*refCountedMutex)
		rcm.RLock()
		if !rcm.collected {
			break
		}
		// See comment in Lock().
		rcm.RUnlock()
	}
	return func() { rcm.RUnlock() }
}

func (p *perKeyMutex) Close() error {
	close(p.closed)
	return nil
}

// For testing only.
func (p *perKeyMutex) count() int {
	count := 0
	p.mutexes.Range(func(key, value any) bool {
		count++
		return true
	})
	return count
}
