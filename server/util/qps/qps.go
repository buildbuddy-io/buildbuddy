package qps

import (
	"sync"
	"sync/atomic"
	"time"
)

const window = time.Minute

type Counter struct {
	counts [60]uint64
	once   *sync.Once
}

func NewCounter() *Counter {
	return &Counter{
		once: &sync.Once{},
	}
}

func (c *Counter) Get() uint64 {
	sum := uint64(0)
	nonZeroBuckets := -1 // don't count the active bucket
	for _, b := range c.counts {
		v := atomic.LoadUint64(&b)
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
		i := (t + 1) % len(c.counts)
		atomic.StoreUint64(&c.counts[i], 0)
		time.Sleep(window / time.Duration(len(c.counts)))
	}
}

func (c *Counter) Inc() uint64 {
	c.once.Do(func() {
		go c.reset()
	})

	i := time.Now().Second() % len(c.counts)
	return atomic.AddUint64(&c.counts[i], 1)
}
