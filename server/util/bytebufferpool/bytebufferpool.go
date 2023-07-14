package bytebufferpool

import (
	"math/bits"
	"sync"
)

// safeSyncPool is a wrapper around `sync.Pool` that provides type safety
// using Go generics.
type safeSyncPool[T any] struct {
	pool sync.Pool
}

func newSafeSyncPool[T any](newFn func() T) safeSyncPool[T] {
	return safeSyncPool[T]{
		pool: sync.Pool{
			New: func() interface{} {
				return newFn()
			},
		},
	}
}

func (p *safeSyncPool[T]) Get() T {
	return p.pool.Get().(T)
}

func (p *safeSyncPool[T]) Put(buf T) {
	p.pool.Put(buf)
}

// Pool is a wrapper around `sync.Pool` that manages pool for buffers of different lengths of n (using power of 2).
type Pool struct {
	// pools contain slices of lengths that are powers of 2. the slice itself is indexed by the exponent.
	// index 0 containing a pool of 2^0 length slices, index 1 containing 2^1 length slices and so forth.
	pools         []safeSyncPool[*[]byte]
	maxBufferSize int
}

func New(maxBufferSize int) *Pool {
	bp := &Pool{}
	for size := 1; ; size *= 2 {
		size := size
		bp.pools = append(bp.pools, newSafeSyncPool(
			func() *[]byte {
				a := make([]byte, size)
				return &a
			},
		))
		if size >= maxBufferSize {
			break
		}
	}
	return bp
}

// Get returns a byte slice of at least the specified length.
//
// CAUTION: In most cases the returned slice will have a greater than requested length. Don't use this package if you
// need buffers of exact lengths.
func (bp *Pool) Get(length int64) []byte {
	// Calculate the smallest power of 2 exponent x where 2^x >= length
	idx := bits.Len64(uint64(length - 1))
	if length == 0 {
		idx = 0
	}
	if idx >= len(bp.pools) {
		idx = len(bp.pools) - 1
	}
	return *bp.pools[idx].Get()
}

// Put returns a byte slice back into the pool.
func (bp *Pool) Put(buf []byte) {
	// Calculate the largest power of 2 exponent x where 2^x <= length
	idx := bits.Len64(uint64(len(buf))) - 1
	if idx < 0 {
		return
	}
	if idx >= len(bp.pools) {
		idx = len(bp.pools) - 1
	}
	bp.pools[idx].Put(&buf)
}
