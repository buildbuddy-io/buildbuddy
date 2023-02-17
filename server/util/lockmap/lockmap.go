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
	sync.RWMutex
	count refCount
}

func newRefCountedMutex() *refCountedMutex {
	var i int64
	return &refCountedMutex{
		RWMutex: sync.RWMutex{},
		count:   refCount{&i},
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
	bigLock sync.RWMutex
}

func New() *perKeyMutex {
	pkm := &perKeyMutex{}
	go pkm.gc()
	return pkm
}

func (p *perKeyMutex) gc() {
	for {
		p.mutexes.Range(func(key, value any) bool {
			p.bigLock.Lock()
			rcm := value.(*refCountedMutex)
			if rcm.count.Val() == 0 {
				p.mutexes.Delete(key)
			}
			p.bigLock.Unlock()
			return true
		})
		time.Sleep(100 * time.Millisecond)
	}
}

func (p *perKeyMutex) Lock(key string) func() {
	p.bigLock.Lock()
	value, _ := p.mutexes.LoadOrStore(key, newRefCountedMutex())
	rcm := value.(*refCountedMutex)
	rcm.count.Inc()
	p.bigLock.Unlock()

	rcm.Lock()
	return func() {
		rcm.Unlock()
		rcm.count.Dec()
	}
}

func (p *perKeyMutex) RLock(key string) func() {
	p.bigLock.Lock()
	value, _ := p.mutexes.LoadOrStore(key, newRefCountedMutex())
	rcm := value.(*refCountedMutex)
	rcm.count.Inc()
	p.bigLock.Unlock()

	rcm.RLock()
	return func() {
		rcm.RUnlock()
		rcm.count.Dec()
	}
}
