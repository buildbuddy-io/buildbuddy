package qps

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
)

type bin struct {
	c atomic.Uint64
}

func (b *bin) Get() uint64 {
	return b.c.Load()
}
func (b *bin) Add(d uint64) {
	b.c.Add(d)
}
func (b *bin) Inc() {
	b.Add(1)
}
func (b *bin) Reset() {
	b.c.Store(0)
}

type Counter struct {
	counts [60]bin
	idx    atomic.Uint64
	// The number of bins that should be included in the average. After the
	// first full averaging period elapses, this will always equal len(counts).
	nValidBins atomic.Uint64
	window     time.Duration
	clock      clockwork.Clock
	startOnce  sync.Once
	stop       chan struct{}
}

// NewCounter returns a QPS counter using the given duration as the averaging
// window. The caller must call Stop() on the returned counter when it is no
// longer needed.
func NewCounter(window time.Duration, clock clockwork.Clock) *Counter {
	c := &Counter{
		window: window,
		clock:  clock,
		stop:   make(chan struct{}),
	}
	c.nValidBins.Store(1)
	return c
}

func (c *Counter) bin(idx int) *bin {
	b := &(c.counts[idx])
	return b
}

func (c *Counter) currentBin() *bin {
	idx := c.idx.Load()
	return c.bin(int(idx))
}

func (c *Counter) Get() float64 {
	sum := uint64(0)
	nValidBins := c.nValidBins.Load()
	for i := 0; i < int(nValidBins); i++ {
		sum += c.bin(i).Get()
	}
	binDurationSec := float64(c.window) * 1e-9 / float64(len(c.counts))
	summedDurationSec := binDurationSec * float64(nValidBins)
	qps := float64(sum) / float64(summedDurationSec)
	return qps
}

// Advances to the next bin, resets its current count, and marks it valid if
// it is still marked invalid.
func (c *Counter) update() {
	idx := c.idx.Load()
	idx = (idx + 1) % uint64(len(c.counts))
	c.idx.Store(idx)

	c.bin(int(idx)).Reset()

	nv := c.nValidBins.Load()
	nv = min(nv+1, uint64(len(c.counts)))
	c.nValidBins.Store(nv)
}

func (c *Counter) start() {
	t := c.clock.NewTicker(time.Duration(float64(c.window) / float64(len(c.counts))))
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.Chan():
		}
		c.update()
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
