package testhttp

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
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
