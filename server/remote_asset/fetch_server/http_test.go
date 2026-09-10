package fetch_server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"

	"github.com/stretchr/testify/require"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

func TestFetchHTTPRetries(t *testing.T) {
	for _, tc := range []struct {
		name     string
		outcomes []int // 0 represents a transport failure.
		code     gcodes.Code
	}{
		{"TLS_timeout_then_success", []int{0, 200}, gcodes.OK},
		{"exhaust_transport", []int{0, 0, 0}, gcodes.Unavailable},
		{"server_errors_then_success", []int{502, 503, 200}, gcodes.OK},
		{"exhaust_server_errors", []int{503, 503, 503}, gcodes.Unavailable},
		{"rate_limit_then_success", []int{429, 200}, gcodes.OK},
		{"request_timeout_then_success", []int{408, 200}, gcodes.OK},
		{"not_found", []int{404}, gcodes.NotFound},
		{"forbidden", []int{403}, gcodes.NotFound},
		{"bad_request", []int{400}, gcodes.NotFound},
		{"transient_then_terminal", []int{503, 404}, gcodes.NotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			var bodies []*trackedBody
			client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				for _, body := range bodies {
					require.True(t, body.closed, "failed response must be closed before retry")
				}
				require.Less(t, attempts, len(tc.outcomes))
				outcome := tc.outcomes[attempts]
				attempts++
				require.Equal(t, "secret", r.Header.Get("Authorization"))
				if outcome == 0 {
					return nil, errors.New("net/http: TLS handshake timeout")
				}
				body := &trackedBody{Reader: strings.NewReader("body")}
				bodies = append(bodies, body)
				return &http.Response{StatusCode: outcome, Status: fmt.Sprint(outcome), Body: body, Header: make(http.Header)}, nil
			})}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/asset", nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "secret")
			rsp, err := fetchHTTP(req, client)
			require.Equal(t, tc.code, gstatus.Code(err), "%v", err)
			require.Equal(t, len(tc.outcomes), attempts)
			if tc.code == gcodes.OK {
				require.NotNil(t, rsp)
				require.False(t, bodies[len(bodies)-1].closed)
				rsp.Body.Close()
			} else {
				require.Nil(t, rsp)
			}
			for _, body := range bodies {
				require.True(t, body.closed)
			}
		})
	}
}

func TestFetchHTTPContext(t *testing.T) {
	for _, mode := range []string{"already_canceled", "during_request", "during_backoff", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wantCode := gcodes.Canceled
			if mode == "deadline" {
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer deadlineCancel()
				wantCode = gcodes.DeadlineExceeded
			}
			attempts := 0
			returned := make(chan struct{})
			client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				attempts++
				switch mode {
				case "during_request":
					cancel()
				case "during_backoff":
					close(returned)
				case "deadline":
					<-r.Context().Done()
				}
				return nil, errors.New("connection reset")
			})}
			if mode == "already_canceled" {
				cancel()
			}
			if mode == "during_backoff" {
				go func() { <-returned; time.Sleep(10 * time.Millisecond); cancel() }()
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/asset", nil)
			require.NoError(t, err)
			rsp, err := fetchHTTP(req, client)
			require.Nil(t, rsp)
			require.Equal(t, wantCode, gstatus.Code(err))
			if mode == "already_canceled" {
				require.Zero(t, attempts)
			} else {
				require.Equal(t, 1, attempts)
			}
		})
	}
}

func TestFetchHTTPDoesNotRetryPermanentTransportErrors(t *testing.T) {
	for _, failure := range []error{httpclient.ErrIPNotAllowed, &net.DNSError{Err: "no such host", IsNotFound: true}} {
		t.Run(failure.Error(), func(t *testing.T) {
			attempts := 0
			client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				attempts++
				return nil, failure
			})}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/asset", nil)
			require.NoError(t, err)
			rsp, err := fetchHTTP(req, client)
			require.Nil(t, rsp)
			require.Equal(t, gcodes.NotFound, gstatus.Code(err))
			require.Equal(t, 1, attempts)
		})
	}
}
