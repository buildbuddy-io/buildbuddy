package bytebufferpool

import (
	"math/bits"
	"sync"
)

type FixedSizePool struct {
	pool sync.Pool
}

func (p *FixedSizePool) Get() []byte {
	return *(p.pool.Get().(*[]byte))
}

func (p *FixedSizePool) Put(buf []byte) {
	p.pool.Put(&buf)
}

// FixedSize returns a byte buffer pool with fixed size buffers. This is a
// type-safe wrapper around sync.Pool.
func FixedSize(bufferSize int) *FixedSizePool {
	return &FixedSizePool{
		sync.Pool{
			New: func() any {
				buf := make([]byte, bufferSize)
				return &buf
			},
		},
	}
}

type VariableSizePool struct {
	// pools contain slices of capacities that are powers of 2. the slice itself
	// is indexed by the exponent.
	// index 0 containing a pool of 2^0 capacity slices, index 1 containing 2^1
	// capacity slices and so forth.
	pools         []*FixedSizePool
	maxBufferSize int
}

// VariableSize returns a byte buffer pool that can be used when the buffer
// size is not fixed. It internally maintains pools of different buffer sizes to
// accommodate requests of different sizes.
func VariableSize(maxBufferSize int) *VariableSizePool {
	bp := &VariableSizePool{}
	for size := 1; ; size *= 2 {
		size := size
		bp.pools = append(bp.pools, FixedSize(size))
		if size >= maxBufferSize {
			break
		}
	}
	return bp
}

// Get returns a byte slice of the specified length, up to the maximum length
// configured in the pool.
func (bp *VariableSizePool) Get(length int64) []byte {
	// Calculate the smallest power of 2 exponent x where 2^x >= length
	idx := bits.Len64(uint64(length - 1))
	if length == 0 {
		idx = 0
	}
	if idx >= len(bp.pools) {
		idx = len(bp.pools) - 1
	}
	buf := bp.pools[idx].Get()
	if length > int64(cap(buf)) {
		length = int64(cap(buf))
	}
	return buf[:length]
}

// Put returns a byte slice back into the pool.
func (bp *VariableSizePool) Put(buf []byte) {
	// Calculate the largest power of 2 exponent x where 2^x <= cap
	idx := bits.Len64(uint64(cap(buf))) - 1
	if idx < 0 {
		return
	}
	if idx >= len(bp.pools) {
		idx = len(bp.pools) - 1
	}
	bp.pools[idx].Put(buf)
}
