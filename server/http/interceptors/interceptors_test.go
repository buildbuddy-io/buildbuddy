package interceptors

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedirectIfNotForwardedHTTPS(t *testing.T) {
	flags.Set(t, "ssl.upgrade_insecure", true)

	tests := []struct {
		name            string
		route           string
		expectedCode    int
		expectedHeaders http.Header
		setup           func(*http.Request)
	}{
		{
			name:            "https request",
			route:           "/foo",
			expectedCode:    http.StatusOK,
			expectedHeaders: http.Header{},
			setup: func(req *http.Request) {
				req.Header.Set("X-Forwarded-Proto", "https")
			},
		},
		{
			name:         "http request with X-Forwarded-Proto header",
			route:        "/foo",
			expectedCode: http.StatusMovedPermanently,
			expectedHeaders: http.Header{
				"Location": []string{"https://example.com/foo"},
			},
			setup: func(req *http.Request) {
				req.Header.Set("X-Forwarded-Proto", "http")
			},
		},
		{
			name:         "http request without X-Forwarded-Proto header",
			route:        "/foo",
			expectedCode: http.StatusMovedPermanently,
			expectedHeaders: http.Header{
				"Location": []string{"https://example.com/foo"},
			},
			setup: func(req *http.Request) {
				req.Header.Del("X-Forwarded-Proto")
			},
		},
		{
			name:            "healthcheck request without X-Forwarded-Proto header",
			route:           "/health",
			expectedCode:    http.StatusOK,
			expectedHeaders: http.Header{},
			setup: func(req *http.Request) {
				req.Header.Del("X-Forwarded-Proto")
				req.Header.Set("User-Agent", "GoogleHC/1.0")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.route, nil)
			if tt.setup != nil {
				tt.setup(req)
			}

			rr := httptest.NewRecorder()
			RedirectIfNotForwardedHTTPS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, r.Header)
			})).ServeHTTP(rr, req)

			require.Equal(t, tt.expectedCode, rr.Code)

			for headerName, expectedValues := range tt.expectedHeaders {
				assert.Equal(t, expectedValues, rr.Header().Values(headerName))
			}
		})
	}
}
