// Package testhttp provides HTTP test utilities.
package testhttp

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"
)

func NewServer(t *testing.T) (*url.URL, net.Listener) {
	port := testport.FindFree(t)

	addr := fmt.Sprintf("localhost:%d", port)
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err, "failed to start test HTTP server")

	log.Debugf("Test HTTP server listening on %s", addr)

	t.Cleanup(func() {
		err := lis.Close()
		require.NoError(t, err)
	})
	return &url.URL{Scheme: "http", Host: addr}, lis
}

// StartServer runs a test-scoped HTTP server and returns the base server URL
// in the format "http://localhost:PORT"
func StartServer(t *testing.T, handler http.Handler) *url.URL {
	u, lis := NewServer(t)
	go http.Serve(lis, handler)
	return u
}

// ResponseWriter is a http.ResponseWriter that fails the test if an
// HTTP error code is written, but otherwise discards all data written to it.
type ResponseWriter struct {
	t      *testing.T
	header map[string][]string
}

func NewResponseWriter(t *testing.T) *ResponseWriter {
	return &ResponseWriter{
		t:      t,
		header: make(map[string][]string),
	}
}

func (w *ResponseWriter) WriteHeader(statusCode int) {
	require.Less(w.t, statusCode, 400, "Wrote HTTP status %d", statusCode)
}
func (w *ResponseWriter) Header() http.Header {
	return w.header
}
func (w *ResponseWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

// RequestCounter counts HTTP requests by "METHOD PATH" key.
type RequestCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func NewRequestCounter() *RequestCounter {
	return &RequestCounter{counts: make(map[string]int)}
}

func (c *RequestCounter) Inc(r *http.Request) {
	c.mu.Lock()
	c.counts[r.Method+" "+r.URL.Path]++
	c.mu.Unlock()
}

func (c *RequestCounter) Snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap := make(map[string]int, len(c.counts))
	for k, v := range c.counts {
		snap[k] = v
	}
	return snap
}

func (c *RequestCounter) Reset() {
	c.mu.Lock()
	c.counts = map[string]int{}
	c.mu.Unlock()
}

// Director selects the backend origin for an HTTP request.
type Director func(req *http.Request) *url.URL

// Proxy is an in-process HTTP reverse proxy for tests.
type Proxy struct {
	*httptest.Server

	mu       sync.RWMutex
	director Director
}

// StartProxy starts a test-scoped proxy using director to select a backend for
// each request.
func StartProxy(t *testing.T, director Director) *Proxy {
	p := &Proxy{director: director}
	p.Server = httptest.NewServer(&httputil.ReverseProxy{
		Director: p.direct,
	})
	t.Cleanup(p.Close)
	return p
}

func (p *Proxy) direct(req *http.Request) {
	p.mu.RLock()
	director := p.director
	p.mu.RUnlock()
	target := director(req)
	req.URL.Scheme = target.Scheme
	req.URL.Host = target.Host
}

// SetDirector changes how subsequent requests select a backend.
func (p *Proxy) SetDirector(director Director) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.director = director
}
