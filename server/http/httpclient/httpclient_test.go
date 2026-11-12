package httpclient

import (
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestBlockingDialerControl(t *testing.T) {
	tests := []struct {
		name           string
		address        string
		allowLocalhost bool
		wantError      bool
	}{
		// Public addresses should be allowed by default.
		{
			name:      "public address",
			address:   "8.8.8.8:80",
			wantError: false,
		},
		// IPv4 and IPv6 loopback addresses should only be allowed when the
		// localhost flag is enabled.
		{
			name:      "localhost",
			address:   "127.0.0.1:80",
			wantError: true,
		},
		{
			name:           "localhost allowed by flag",
			address:        "127.0.0.1:80",
			allowLocalhost: true,
			wantError:      false,
		},
		{
			name:      "IPv6 localhost",
			address:   "[::1]:80",
			wantError: true,
		},
		{
			name:           "IPv6 localhost allowed by flag",
			address:        "[::1]:80",
			allowLocalhost: true,
			wantError:      false,
		},
		// Private and link-local addresses should always be blocked.
		{
			name:      "IPv4 private address",
			address:   "10.0.0.1:80",
			wantError: true,
		},
		{
			name:      "IPv6 private address",
			address:   "[fd00::1]:80",
			wantError: true,
		},
		{
			name:      "IPv4 link-local address",
			address:   "169.254.169.254:80",
			wantError: true,
		},
		{
			name:      "IPv6 link-local address",
			address:   "[fe80::1]:80",
			wantError: true,
		},
		{
			name:           "IPv4 unspecified address even with localhost allowed",
			address:        "0.0.0.0:80",
			allowLocalhost: true,
			wantError:      true,
		},
		{
			name:      "IPv4 unspecified address",
			address:   "0.0.0.0:80",
			wantError: true,
		},
		{
			name:      "IPv6 unspecified address",
			address:   "[::]:80",
			wantError: true,
		},
		{
			name:      "IPv4 broadcast address",
			address:   "255.255.255.255:80",
			wantError: true,
		},
		{
			name:      "IPv6 multicast address",
			address:   "[ff02::1]:80",
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			flags.Set(t, "http.client.allow_localhost", test.allowLocalhost)
			err := blockingDialerControl(nil)("tcp", test.address, nil)
			if test.wantError {
				require.ErrorContains(t, err, "IP address not allowed")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestFollowRedirectsToAllowedIPs(t *testing.T) {
	_, allowed, err := net.ParseCIDR("10.0.0.0/8")
	require.NoError(t, err)
	inner := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: req}
		if req.URL.Path == "/v2/blob" {
			resp.StatusCode = http.StatusFound
			resp.Header.Set("Location", req.URL.Query().Get("to"))
		}
		return resp, nil
	})
	for _, tc := range []struct {
		name       string
		allowed    []*net.IPNet
		to         string
		wantStatus int
	}{
		{name: "allowlisted IP is followed", allowed: []*net.IPNet{allowed}, to: "https://10.1.2.3/blob", wantStatus: http.StatusOK},
		{name: "other IP is left to the caller", allowed: []*net.IPNet{allowed}, to: "https://192.168.1.1/blob", wantStatus: http.StatusFound},
		{name: "empty allowlist is a no-op", to: "https://10.1.2.3/blob", wantStatus: http.StatusFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := FollowRedirectsToAllowedIPs(inner, tc.allowed)
			req, err := http.NewRequest(http.MethodGet, "https://registry.example/v2/blob?to="+tc.to, nil)
			require.NoError(t, err)
			resp, err := tr.RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, tc.wantStatus, resp.StatusCode)
		})
	}
}
