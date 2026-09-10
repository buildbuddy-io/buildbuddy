package fetch_server

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

func TestHTTPFetchError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code gcodes.Code
	}{
		{"blocked_ip", httpclient.ErrIPNotAllowed, gcodes.NotFound},
		{"nxdomain", &net.DNSError{Err: "no such host", IsNotFound: true}, gcodes.NotFound},
		{"dns_timeout", &net.DNSError{Err: "timeout", IsTimeout: true}, gcodes.Unavailable},
		{"dns_temporary", &net.DNSError{Err: "server failure", IsTemporary: true}, gcodes.Unavailable},
		{"TLS_timeout", errors.New("net/http: TLS handshake timeout"), gcodes.Unavailable},
		{"connection_reset", errors.New("connection reset by peer"), gcodes.Unavailable},
		{"redirect_rejected", status.NotFoundError("stopped after 10 redirects"), gcodes.NotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// net/http wraps dial failures through both net.OpError and url.Error.
			err := &url.Error{Op: "Get", URL: "https://example.com/asset", Err: &net.OpError{Op: "dial", Net: "tcp", Err: tc.err}}
			classified := httpFetchError(err.URL, err)
			require.Equal(t, tc.code, gstatus.Code(classified))
			require.Contains(t, status.Message(classified), err.URL)
			require.Contains(t, status.Message(classified), tc.err.Error())
		})
	}
}

func TestValidateHTTPURL(t *testing.T) {
	for _, uri := range []string{"ftp://example.com/a", "file:///tmp/a", "relative/path", "//example.com/a", "http:///path"} {
		t.Run(uri, func(t *testing.T) {
			u, err := url.Parse(uri)
			require.NoError(t, err)
			require.Equal(t, gcodes.NotFound, gstatus.Code(validateHTTPURL(u)))
		})
	}
	for _, uri := range []string{"https://example.com/a", "http://example.com/a"} {
		u, err := url.Parse(uri)
		require.NoError(t, err)
		require.NoError(t, validateHTTPURL(u))
	}
}

func TestFetchTimeoutErrorPreservesLastAttempt(t *testing.T) {
	// The budget may expire between a 404 response and the next mirror. Keep
	// the 404 diagnostic while making clear that not all mirrors were tried.
	err := fetchTimeoutError(1, 2, fmt.Errorf("https://example.com/first: %w", status.NotFoundError("HTTP 404 Not Found")))
	require.Equal(t, gcodes.DeadlineExceeded, gstatus.Code(err))
	require.Contains(t, status.Message(err), "attempting 1 of 2 URIs")
	require.Contains(t, status.Message(err), "https://example.com/first")
	require.Contains(t, status.Message(err), "404 Not Found")
	err = fetchTimeoutError(0, 2, nil)
	require.Contains(t, status.Message(err), "attempting 0 of 2 URIs")
	require.NotContains(t, status.Message(err), "last fetch error")
}
