package bytebufferpool

import (
	"math/bits"
	"sync"
)

// Pool is a wrapper around `sync.Pool` that manages pool for buffers of different lengths of n (using power of 2).
type Pool struct {
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

// Get returns a byte slice with a length of at least `length`.
func (bp *Pool) Get(length int64) []byte {
	idx := bits.Len64(uint64(length - 1))
	if length == 0 {
		idx = 0
	}
	if idx >= len(bp.pools) {
		idx = len(bp.pools) - 1
	}
	return bp.pools[idx].Get().([]byte)
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
