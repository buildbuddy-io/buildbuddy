package bytebufferpool

import (
	"math/bits"
	"sync"
)

// Pool is a wrapper around `sync.Pool` that manages pool for buffers of different lengths of n (using power of 2).
type Pool struct {
	// pools contain slices of lengths that are powers of 2. the slice itself is indexed by the exponent.
	// index 0 containing a pool of 2^0 length slices, index 1 containing 2^1 length slices and so forth.
	// To compute the index for a given length we use bits.Len64(n - 1) which computes the inverse of 2^n.
	pools         []sync.Pool
	maxBufferSize int
}

func New(maxBufferSize int) *Pool {
	bp := &Pool{}
	for size := 1; ; size *= 2 {
		size := size
		bp.pools = append(bp.pools, sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		})
		if size >= maxBufferSize {
			break
		}
	}
	return bp
}

// Get returns a byte slice of the requested length.
func (bp *Pool) Get(length int64) []byte {
	idx := bits.Len64(uint64(length - 1))
	if length == 0 {
		idx = 0
	}
	if idx >= len(bp.pools) {
		idx = len(bp.pools) - 1
	}
	buf := bp.pools[idx].Get().([]byte)
	return buf[:length]
}

// Put returns a byte slice back into the pool.
func (bp *Pool) Put(buf []byte) {
	idx := bits.Len64(uint64(len(buf) - 1))
	if idx < 0 {
		return
	}
	if idx >= len(bp.pools) {
		idx = len(bp.pools) - 1
	}
	bp.pools[idx].Put(buf)
}
