package qps

import (
	"sync"
	"sync/atomic"
	"time"
)

const window = time.Minute

type bin struct {
	c uint64
}

func (b *bin) Get() uint64 {
	return atomic.LoadUint64(&b.c)
}
func (b *bin) Add(d uint64) {
	atomic.AddUint64(&b.c, d)
}
func (b *bin) Inc() {
	b.Add(1)
}
func (b *bin) Reset() {
	atomic.StoreUint64(&b.c, 0)
}

type Counter struct {
	counts [60]bin
	once   *sync.Once
	idx    uint64
}

func NewCounter() *Counter {
	c := &Counter{
		once: &sync.Once{},
	}
	return c
}

func (c *Counter) bin(idx int) *bin {
	b := &(c.counts[idx])
	return b
}

func (c *Counter) currentBin() *bin {
	idx := atomic.LoadUint64(&c.idx)
	return c.bin(int(idx))
}

func (c *Counter) Get() uint64 {
	sum := uint64(0)
	nonZeroBuckets := -1 // don't count the active bucket
	for i := 0; i < len(c.counts); i++ {
		v := c.bin(i).Get()

		if v > 0 {
			sum += v
			nonZeroBuckets += 1
		}
	}
	if nonZeroBuckets <= 0 {
		return 0
	}
	qps := uint64(float64(sum) / float64(nonZeroBuckets))
	return qps
}

func (c *Counter) reset() {
	for {
		t := time.Now().Second()
		numCounts := len(c.counts)
		atomic.StoreUint64(&c.idx, uint64(t%numCounts))
		j := (t + 1) % numCounts
		c.bin(j).Reset()

		time.Sleep(window / time.Duration(numCounts))
	}
}

func (c *Counter) Inc() {
	c.once.Do(func() {
		go c.reset()
	})
	c.currentBin().Inc()
}
