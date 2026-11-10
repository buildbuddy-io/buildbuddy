package httpclient

import (
	"fmt"
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

type fakeRoundTripper map[string]*http.Response

func (f fakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r, ok := f[req.URL.String()]
	if !ok {
		return nil, fmt.Errorf("unexpected request to %s", req.URL)
	}
	resp := *r
	resp.Header = r.Header.Clone()
	resp.Request = req
	if resp.Body == nil {
		resp.Body = io.NopCloser(strings.NewReader(""))
	}
	return &resp, nil
}

func TestFollowRedirectsToAllowedIPs(t *testing.T) {
	response := func(code int, body string, headers ...string) *http.Response {
		h := http.Header{}
		for i := 0; i < len(headers); i += 2 {
			h.Set(headers[i], headers[i+1])
		}
		return &http.Response{StatusCode: code, Header: h, Body: io.NopCloser(strings.NewReader(body))}
	}
	const registryURL = "https://registry.example/v2/img/blobs/sha256:abc"
	for _, tc := range []struct {
		name        string
		start       string // defaults to registryURL
		responses   fakeRoundTripper
		wantStatus  int
		wantHeaders http.Header
		wantBody    string
		wantErr     string
	}{
		{
			name: "follows redirect to allowlisted IP and strips WWW-Authenticate",
			responses: fakeRoundTripper{
				registryURL:             response(http.StatusFound, "", "Location", "https://10.1.2.3/blob"),
				"https://10.1.2.3/blob": response(http.StatusOK, "blob contents", "WWW-Authenticate", `Bearer realm="storage"`),
			},
			wantStatus:  http.StatusOK,
			wantHeaders: http.Header{},
			wantBody:    "blob contents",
		},
		{
			name:  "keeps WWW-Authenticate after same-host redirect",
			start: "https://10.1.2.3/v2/img/blobs/sha256:abc",
			responses: fakeRoundTripper{
				"https://10.1.2.3/v2/img/blobs/sha256:abc": response(http.StatusFound, "", "Location", "/storage/blob"),
				"https://10.1.2.3/storage/blob":            response(http.StatusUnauthorized, "", "WWW-Authenticate", `Bearer realm="registry"`),
			},
			wantStatus:  http.StatusUnauthorized,
			wantHeaders: http.Header{"Www-Authenticate": {`Bearer realm="registry"`}},
		},
		{
			name: "leaves redirect to non-allowlisted IP untouched",
			responses: fakeRoundTripper{
				registryURL: response(http.StatusFound, "", "Location", "https://192.168.1.1/blob", "WWW-Authenticate", "Basic"),
			},
			wantStatus:  http.StatusFound,
			wantHeaders: http.Header{"Location": {"https://192.168.1.1/blob"}, "Www-Authenticate": {"Basic"}},
		},
		{
			name: "returns redirect to non-allowlisted IP after an allowlisted hop",
			responses: fakeRoundTripper{
				registryURL:             response(http.StatusFound, "", "Location", "https://10.1.2.3/blob"),
				"https://10.1.2.3/blob": response(http.StatusFound, "", "Location", "//192.168.1.1/blob"),
			},
			wantStatus:  http.StatusFound,
			wantHeaders: http.Header{"Location": {"https://192.168.1.1/blob"}},
		},
		{
			name: "leaves redirect to hostname untouched",
			responses: fakeRoundTripper{
				registryURL: response(http.StatusFound, "", "Location", "https://storage.example/blob"),
			},
			wantStatus:  http.StatusFound,
			wantHeaders: http.Header{"Location": {"https://storage.example/blob"}},
		},
		{
			name: "makes relative Location of returned redirect absolute after a hop",
			responses: fakeRoundTripper{
				registryURL:               response(http.StatusFound, "", "Location", "https://10.1.2.3/a/blob"),
				"https://10.1.2.3/a/blob": response(http.StatusMultipleChoices, "", "Location", "other"),
			},
			wantStatus:  http.StatusMultipleChoices,
			wantHeaders: http.Header{"Location": {"https://10.1.2.3/a/other"}},
		},
		{
			name: "stops following an allowlisted redirect loop",
			responses: fakeRoundTripper{
				registryURL:             {StatusCode: http.StatusFound, Header: http.Header{"Location": {"https://10.1.2.3/loop"}}},
				"https://10.1.2.3/loop": {StatusCode: http.StatusFound, Header: http.Header{"Location": {"https://10.1.2.3/loop"}}},
			},
			wantErr: "stopped after 10 redirects",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, allowed, err := net.ParseCIDR("10.0.0.0/8")
			require.NoError(t, err)
			tr := FollowRedirectsToAllowedIPs(tc.responses, []*net.IPNet{allowed})
			start := tc.start
			if start == "" {
				start = registryURL
			}
			req, err := http.NewRequest(http.MethodGet, start, nil)
			require.NoError(t, err)
			resp, err := tr.RoundTrip(req)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, tc.wantStatus, resp.StatusCode)
			require.Equal(t, tc.wantHeaders, resp.Header)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.wantBody, string(body))
		})
	}
}
