package qps

import (
	"sync"
	"sync/atomic"
	"time"
)

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
	idx    uint64
	// The number of bins that should be included in the average. After the
	// first full averaging period elapses, this will always equal len(counts).
	nValidBins uint64
	window     time.Duration
	startOnce  sync.Once
	stop       chan struct{}
}

// NewCounter returns a QPS counter using the given duration as the averaging
// window. The caller must call Stop() on the returned counter when it is no
// longer needed.
func NewCounter(window time.Duration) *Counter {
	return &Counter{
		nValidBins: 1,
		window:     window,
		stop:       make(chan struct{}),
	}
}

func (c *Counter) bin(idx int) *bin {
	b := &(c.counts[idx])
	return b
}

func (c *Counter) currentBin() *bin {
	idx := atomic.LoadUint64(&c.idx)
	return c.bin(int(idx))
}

func (c *Counter) Get() float64 {
	sum := uint64(0)
	nValidBins := atomic.LoadUint64(&c.nValidBins)
	for i := 0; i < int(nValidBins); i++ {
		sum += c.bin(i).Get()
	}
	binDurationSec := float64(c.window) * 1e-9 / float64(len(c.counts))
	summedDurationSec := binDurationSec * float64(nValidBins)
	qps := float64(sum) / float64(summedDurationSec)
	return qps
}

func (c *Counter) start() {
	t := time.NewTicker(time.Duration(float64(c.window) / float64(len(c.counts))))
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
		}
		// Advance to the next bin, reset its current count, and mark it valid
		// if we haven't done so already.
		idx := atomic.LoadUint64(&c.idx)
		idx = (idx + 1) % uint64(len(c.counts))
		atomic.StoreUint64(&c.idx, idx)

		c.bin(int(idx)).Reset()

		nv := atomic.LoadUint64(&c.nValidBins)
		nv = min(nv+1, uint64(len(c.counts)))
		atomic.StoreUint64(&c.nValidBins, nv)
	}
}

func (c *Counter) Stop() {
	close(c.stop)
}

func (c *Counter) Inc() {
	c.startOnce.Do(func() {
		go c.start()
	})
	c.currentBin().Inc()
}
