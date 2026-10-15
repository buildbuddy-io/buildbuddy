package flagz_test

import (
	stdflag "flag"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagz"
	"github.com/stretchr/testify/require"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

var testFlag = flag.String("flagz_test.flag", "original", "A flag for testing flagz.")

var ignoreTestFlagsOnce sync.Once

// ignoreTestFlags excludes the stdlib testing flags, which can't be rendered
// to YAML. They are only registered once the test binary starts running.
func ignoreTestFlags() {
	ignoreTestFlagsOnce.Do(func() {
		stdflag.VisitAll(func(f *stdflag.Flag) {
			if strings.HasPrefix(f.Name, "test.") {
				flagyaml.IgnoreFlagForYAML(f.Name)
			}
		})
	})
}

func TestGetRendersFlags(t *testing.T) {
	ignoreTestFlags()
	rec := httptest.NewRecorder()
	flagz.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/flagz", nil))

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "flagz_test")
}

func TestRejectedRequestsLeaveFlagUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name         string
		method       string
		target       string
		body         string
		expectedCode int
	}{
		{"get with query param", http.MethodGet, "/flagz?flagz_test.flag=changed", "", http.StatusBadRequest},
		{"post with query param", http.MethodPost, "/flagz?flagz_test.flag=changed", "", http.StatusMethodNotAllowed},
		{"malformed percent escape", http.MethodGet, "/flagz?a=100%", "", http.StatusBadRequest},
		{"semicolon separator", http.MethodGet, "/flagz?a=1;b=2", "", http.StatusBadRequest},
		{"post with form body", http.MethodPost, "/flagz", "flagz_test.flag=changed", http.StatusMethodNotAllowed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}
			rec := httptest.NewRecorder()
			flagz.ServeHTTP(rec, req)

			require.Equal(t, tc.expectedCode, rec.Code)
			require.Contains(t, rec.Body.String(), "read-only")
			require.Equal(t, "original", *testFlag)
		})
	}
}
