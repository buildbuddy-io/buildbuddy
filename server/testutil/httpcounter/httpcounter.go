package httpcounter

import (
	"net/http"
	"sync"
)

// Counter tracks HTTP requests by method and path.
type Counter struct {
	mu     sync.Mutex
	counts map[string]int
}

// New creates a new Counter.
func New() *Counter {
	return &Counter{counts: make(map[string]int)}
}

// Inc increments the count for the given request's method and path.
func (c *Counter) Inc(r *http.Request) {
	c.mu.Lock()
	c.counts[r.Method+" "+r.URL.Path]++
	c.mu.Unlock()
}

// Snapshot returns a copy of the current counts.
func (c *Counter) Snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap := make(map[string]int, len(c.counts))
	for k, v := range c.counts {
		snap[k] = v
	}
	return snap
}

// Reset clears all counts.
func (c *Counter) Reset() {
	c.mu.Lock()
	c.counts = map[string]int{}
	c.mu.Unlock()
}
