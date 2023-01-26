package lockmap

import (
	"sync"
	"sync/atomic"
	"time"
)

type refCount struct {
	i *int64
}

func (c *refCount) Inc() int64 {
	j := atomic.AddInt64(c.i, 1)
	return j
}
func (c *refCount) Dec() int64 {
	j := atomic.AddInt64(c.i, -1)
	return j
}
func (c *refCount) Val() int64 {
	j := atomic.LoadInt64(c.i)
	return j
}

type refCountedMutex struct {
	*sync.RWMutex
	*refCount
}

func newRefCountedMutex() *refCountedMutex {
	var i int64
	return &refCountedMutex{
		&sync.RWMutex{},
		&refCount{&i},
	}
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
}

type perKeyMutex struct {
	mutexes sync.Map
	bigLock *sync.RWMutex
	once    *sync.Once
}

func New() *perKeyMutex {
	pkm := perKeyMutex{
		mutexes: sync.Map{},
		bigLock: &sync.RWMutex{},
		once:    &sync.Once{},
	}
	return &pkm
}

func (p *perKeyMutex) gc() {
	for {
		p.mutexes.Range(func(key, value any) bool {
			rcm := value.(*refCountedMutex)
			if rcm.Val() == 0 {
				p.bigLock.Lock()
				p.mutexes.Delete(key)
				p.bigLock.Unlock()
			}
			return true
		})
		time.Sleep(100 * time.Millisecond)
	}
}

func (p *perKeyMutex) Lock(key string) func() {
	p.bigLock.Lock()
	defer p.bigLock.Unlock()

	p.once.Do(func() {
		go p.gc()
	})

	value, _ := p.mutexes.LoadOrStore(key, newRefCountedMutex())
	rcm := value.(*refCountedMutex)

	rcm.Lock()
	rcm.Inc()

	return func() {
		rcm.Dec()
		rcm.Unlock()
	}
}

func (p *perKeyMutex) RLock(key string) func() {
	p.bigLock.Lock()
	defer p.bigLock.Unlock()

	p.once.Do(func() {
		go p.gc()
	})

	value, _ := p.mutexes.LoadOrStore(key, newRefCountedMutex())
	rcm := value.(*refCountedMutex)

	rcm.RLock()
	rcm.Inc()

	return func() {
		rcm.Dec()
		rcm.RUnlock()
	}
}
