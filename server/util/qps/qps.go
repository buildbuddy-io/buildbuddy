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
	for {
		old := atomic.LoadUint64(&b.c)
		new := old + d
		if atomic.CompareAndSwapUint64(&b.c, old, new) {
			return
		}
	}
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
	idx    int
}

func NewCounter() *Counter {
	c := &Counter{
		once: &sync.Once{},
	}
	return c
}

func (c *Counter) Get() uint64 {
	sum := uint64(0)
	nonZeroBuckets := -1 // don't count the active bucket
	for _, b := range c.counts {
		v := b.Get()
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
		c.idx = t % numCounts
		j := (t + 1) % numCounts
		c.counts[j].Reset()

		time.Sleep(window / time.Duration(numCounts))
	}
}

func (c *Counter) Inc() {
	c.once.Do(func() {
		go c.reset()
	})
	c.counts[c.idx].Inc()
}
