package flagz_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagz"
	"github.com/stretchr/testify/require"
)

// registerTestFlag swaps in an isolated flag set containing one flag, since
// the process-wide set includes flags (like the testing package's) that can't
// be rendered to YAML.
func registerTestFlag(t *testing.T) *string {
	common.DefaultFlagSet = flag.NewFlagSet("test", flag.ContinueOnError)
	t.Cleanup(func() { common.DefaultFlagSet = flag.CommandLine })
	return flag.String("flagz_test.flag", "original", "A flag for testing flagz.")
}

func TestGetRendersFlags(t *testing.T) {
	registerTestFlag(t)
	rec := httptest.NewRecorder()
	flagz.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/flagz", nil))

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "flagz_test")
}

func TestQueryParamsRejectedAndFlagUnchanged(t *testing.T) {
	testFlag := registerTestFlag(t)
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		rec := httptest.NewRecorder()
		flagz.ServeHTTP(rec, httptest.NewRequest(method, "/flagz?flagz_test.flag=changed", nil))

		require.Equal(t, http.StatusBadRequest, rec.Code, method)
		require.Contains(t, rec.Body.String(), "read-only")
		require.Equal(t, "original", *testFlag, method)
	}
}
