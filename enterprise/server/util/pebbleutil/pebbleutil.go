package pebbleutil

import (
	"flag"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
)

var (
	warnAboutLeaks = flag.Bool("cache.pebble.warn_about_leaks", true, "If set, warn about leaked DB handles")
)

// IPebbleDB is an interface the covers the methods on a pebble.DB used by our
// code. An interface is required so that the DB leaser can return a pebble.DB
// embedded in a struct that implements this interface.
type IPebbleDB interface {
	pebble.Reader
	pebble.Writer
	io.Closer

	EstimateDiskUsage(start, end []byte) (uint64, error)
	Flush() error
	Metrics() *pebble.Metrics
	NewBatch() *pebble.Batch
	NewIndexedBatch() *pebble.Batch
	NewSnapshot() *pebble.Snapshot
}

// Leaser is an interface implemented by the leaser that allows clients to get
// a leased pebble DB or close the leaser.
type Leaser interface {
	DB() (IPebbleDB, error)
	Close()
	AcquireSplitLock()
	ReleaseSplitLock()
}

type leaser struct {
	db       *pebble.DB
	waiters  sync.WaitGroup
	closedMu sync.Mutex // PROTECTS(closed)
	closed   bool
	splitMu  sync.RWMutex
}

// NewDBLeaser returns a new DB leaser that wraps db and serializes access to
// it.
//
// Clients can call the leaser's DB() method to get a handle to the wrapped
// pebble DB. Clients are then required call *Close* on this handle when done
// to prevent leaks. The DB cannot be closed until all handles are returned.
//
// Once the DB leaser has been closed with Close(), no new handles can be
// acquired, instead an error is returned.
//
// Additionally, if AcquireSplitLock() is called, the leaser will wait for all
// all handles to be returned and prevent prevent additional handles from being
// leased until ReleaseSplitLock() is called.
func NewDBLeaser(db *pebble.DB) Leaser {
	return &leaser{
		db:       db,
		waiters:  sync.WaitGroup{},
		closedMu: sync.Mutex{},
		closed:   false,
		splitMu:  sync.RWMutex{},
	}
}

func (l *leaser) Close() {
	l.closedMu.Lock()
	defer l.closedMu.Unlock()
	if l.closed {
		return
	}
	l.closed = true

	// wait for all db users to finish up.
	l.waiters.Wait()
}

func (l *leaser) AcquireSplitLock() {
	l.splitMu.Lock()
}

func (l *leaser) ReleaseSplitLock() {
	l.splitMu.Unlock()
}

func (l *leaser) DB() (IPebbleDB, error) {
	l.splitMu.RLock()
	defer l.splitMu.RUnlock()

	l.closedMu.Lock()
	defer l.closedMu.Unlock()
	if l.closed {
		return nil, status.FailedPreconditionError("db is closed")
	}

	handle := &refCountedDB{
		l.db,
		newRefCounter(&l.waiters),
	}

	if *warnAboutLeaks {
		location := "unknown"
		if _, file, no, ok := runtime.Caller(1); ok {
			location = fmt.Sprintf("%s:%d", file, no)
		}
		runtime.SetFinalizer(handle, func(h *refCountedDB) {
			if !h.refCounter.closed {
				log.Errorf("DB() handle leak at %s!", location)
			}
		})
	}
	return handle, nil
}

type refCounter struct {
	wg     *sync.WaitGroup
	closed bool
}

func newRefCounter(wg *sync.WaitGroup) *refCounter {
	wg.Add(1)
	return &refCounter{
		wg:     wg,
		closed: false,
	}
}

func (r *refCounter) Close() error {
	if !r.closed {
		r.closed = true
		r.wg.Add(-1)
	}
	return nil
}

type refCountedDB struct {
	*pebble.DB
	*refCounter
}

func (r *refCountedDB) Close() error {
	// Just close the refcounter, not the DB.
	return r.refCounter.Close()
}
