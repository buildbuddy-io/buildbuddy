package region

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_IsRegionalServer(t *testing.T) {
	productionRegions := []Region{
		{Name: "US", Server: "https://app.buildbuddy.io", Subdomains: "https://*.buildbuddy.io"},
		{Name: "Europe", Server: "https://app.europe.buildbuddy.io", Subdomains: "https://*.europe.buildbuddy.io"},
	}
	for _, tc := range []struct {
		name  string
		url   string
		match bool
	}{
		{name: "public US endpoint", url: "https://remote.buildbuddy.io", match: true},
		{name: "public Europe endpoint", url: "https://remote.europe.buildbuddy.io", match: true},
		{name: "US organization with hyphenated slug", url: "https://test-org.buildbuddy.io", match: true},
		{name: "Europe organization with hyphenated slug", url: "https://test-org.europe.buildbuddy.io", match: true},
		{name: "canonical US app", url: "https://app.buildbuddy.io", match: true},
		{name: "canonical Europe app", url: "https://app.europe.buildbuddy.io", match: true},
		{name: "lookalike base domain", url: "https://evilbuildbuddy.io"},
		{name: "lookalike organization domain", url: "https://test-org.evilbuildbuddy.io"},
		{name: "malformed origin", url: "https:///test-org.buildbuddy.io"},
		{name: "missing scheme", url: "test-org.buildbuddy.io"},
		{name: "insecure organization origin", url: "http://test-org.buildbuddy.io"},
		{name: "untrusted domain suffix", url: "https://test-org.buildbuddy.io.evil.example"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.match, isRegionalServer(productionRegions, tc.url))
		})
	}
}

func TestCORS(t *testing.T) {
	productionRegions := []Region{
		{Name: "US", Server: "https://app.buildbuddy.io", Subdomains: "https://*.buildbuddy.io"},
		{Name: "Europe", Server: "https://app.europe.buildbuddy.io", Subdomains: "https://*.europe.buildbuddy.io"},
	}
	for _, tc := range []struct {
		name          string
		regions       []Region
		method        string
		origin        string
		requestURL    string
		allowed       bool
		handlerCalled bool
		vary          bool
	}{
		{
			name:          "global organization page downloads Europe profile",
			regions:       productionRegions,
			method:        "GET",
			origin:        "https://test-org.buildbuddy.io",
			requestURL:    "https://app.europe.buildbuddy.io/file/download?artifact=execution_profile",
			allowed:       true,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "Europe organization page downloads US profile",
			regions:       productionRegions,
			method:        "GET",
			origin:        "https://test-org.europe.buildbuddy.io",
			requestURL:    "https://app.buildbuddy.io/file/download?artifact=execution_profile",
			allowed:       true,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "canonical global app page downloads Europe profile",
			regions:       productionRegions,
			method:        "GET",
			origin:        "https://app.buildbuddy.io",
			requestURL:    "https://app.europe.buildbuddy.io/file/download?artifact=execution_profile",
			allowed:       true,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "canonical Europe app page downloads US profile",
			regions:       productionRegions,
			method:        "GET",
			origin:        "https://app.europe.buildbuddy.io",
			requestURL:    "https://app.buildbuddy.io/file/download?artifact=execution_profile",
			allowed:       true,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment compatibility: configured HTTPS default port",
			regions:       []Region{{Server: "https://regional.example:443/"}},
			method:        "GET",
			origin:        "https://regional.example",
			allowed:       true,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment compatibility: configured HTTP default port",
			regions:       []Region{{Server: "http://regional.example:80"}},
			method:        "GET",
			origin:        "http://regional.example",
			allowed:       true,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment compatibility: configured HTTPS nondefault port",
			regions:       []Region{{Server: "https://regional.example:8443"}},
			method:        "GET",
			origin:        "https://regional.example:8443",
			allowed:       true,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment compatibility: HTTPS wildcard default port",
			regions:       []Region{{Server: "https://app.example.local:443", Subdomains: "https://*.example.local:443"}},
			method:        "GET",
			origin:        "https://test-org.example.local",
			allowed:       true,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment compatibility: HTTP wildcard default port",
			regions:       []Region{{Server: "http://app.example.local:80", Subdomains: "http://*.example.local:80"}},
			method:        "GET",
			origin:        "http://test-org.example.local",
			allowed:       true,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "configuration security boundary: wildcard path",
			regions:       []Region{{Server: "https://app.example.local", Subdomains: "https://*.example.local/path"}},
			method:        "GET",
			origin:        "https://test-org.example.local",
			allowed:       false,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "configuration security boundary: wildcard query",
			regions:       []Region{{Server: "https://app.example.local", Subdomains: "https://*.example.local?query=true"}},
			method:        "GET",
			origin:        "https://test-org.example.local",
			allowed:       false,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "configuration security boundary: wildcard userinfo",
			regions:       []Region{{Server: "https://app.example.local", Subdomains: "https://user@*.example.local"}},
			method:        "GET",
			origin:        "https://test-org.example.local",
			allowed:       false,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "regional head",
			regions:       productionRegions,
			method:        "HEAD",
			origin:        "https://app.europe.buildbuddy.io",
			allowed:       true,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "regional rpc",
			regions:       productionRegions,
			method:        "POST",
			origin:        "https://app.europe.buildbuddy.io",
			allowed:       true,
			handlerCalled: true,
			vary:          true,
		},
		{
			name:    "regional preflight",
			regions: productionRegions,
			method:  "OPTIONS",
			origin:  "https://app.europe.buildbuddy.io",
			allowed: true,
			vary:    true,
		},
		{
			name:          "untrusted download",
			regions:       productionRegions,
			method:        "GET",
			origin:        "https://test-org.europe.buildbuddy.io.evil.example",
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment security: origin with userinfo",
			regions:       []Region{{Server: "https://regional.example:443"}},
			method:        "GET",
			origin:        "https://user@regional.example",
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment security: origin with path",
			regions:       []Region{{Server: "https://regional.example:443"}},
			method:        "GET",
			origin:        "https://regional.example/path",
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment security: origin with trailing slash",
			regions:       []Region{{Server: "https://regional.example:443"}},
			method:        "GET",
			origin:        "https://regional.example/",
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment security: origin with query",
			regions:       []Region{{Server: "https://regional.example:443"}},
			method:        "GET",
			origin:        "https://regional.example?query",
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment security: origin with empty query",
			regions:       []Region{{Server: "https://regional.example:443"}},
			method:        "GET",
			origin:        "https://regional.example?",
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment security: origin with fragment",
			regions:       []Region{{Server: "https://regional.example:443"}},
			method:        "GET",
			origin:        "https://regional.example#fragment",
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment security: insecure grpc origin",
			regions:       []Region{{Server: "https://regional.example:443"}},
			method:        "GET",
			origin:        "grpc://regional.example",
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "private deployment security: wrong port",
			regions:       []Region{{Server: "https://regional.example:443"}},
			method:        "GET",
			origin:        "https://regional.example:8443",
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "untrusted preflight",
			regions:       productionRegions,
			method:        "OPTIONS",
			origin:        "https://evil.example",
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "same origin download",
			regions:       productionRegions,
			method:        "GET",
			handlerCalled: true,
			vary:          true,
		},
		{
			name:          "no configured regions",
			method:        "GET",
			origin:        "https://app.europe.buildbuddy.io",
			handlerCalled: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previousRegions := *regions
			*regions = tc.regions
			t.Cleanup(func() { *regions = previousRegions })
			handlerCalled := false
			handler := CORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handlerCalled = true
				// CORS must leave authentication and authorization to the wrapped handler.
				w.WriteHeader(http.StatusUnauthorized)
			}))
			requestURL := tc.requestURL
			if requestURL == "" {
				requestURL = "/file/download"
			}
			req := httptest.NewRequest(tc.method, requestURL, nil)
			req.Header.Set("Origin", tc.origin)
			req.Header.Set("Access-Control-Request-Method", "GET")
			req.Header.Set("Access-Control-Request-Headers", "x-buildbuddy-trace")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			assert.Equal(t, tc.handlerCalled, handlerCalled)
			if tc.handlerCalled {
				assert.Equal(t, http.StatusUnauthorized, response.Code)
			}
			if tc.allowed {
				assert.Equal(t, tc.origin, response.Header().Get("Access-Control-Allow-Origin"))
				assert.Equal(t, "true", response.Header().Get("Access-Control-Allow-Credentials"))
				if tc.method == "OPTIONS" {
					assert.Equal(t, http.StatusOK, response.Code)
					assert.Contains(t, response.Header().Values("Vary"), "Access-Control-Request-Method")
					assert.Contains(t, response.Header().Values("Vary"), "Access-Control-Request-Headers")
					assert.Equal(t, "GET", response.Header().Get("Access-Control-Allow-Methods"))
					assert.Equal(t, "x-buildbuddy-trace", response.Header().Get("Access-Control-Allow-Headers"))
				}
			} else {
				assert.Empty(t, response.Header().Get("Access-Control-Allow-Origin"))
				assert.Empty(t, response.Header().Get("Access-Control-Allow-Credentials"))
			}
			if tc.vary {
				assert.Contains(t, response.Header().Values("Vary"), "Origin")
			} else {
				assert.Empty(t, response.Header().Values("Vary"))
			}
		})
	}
}
