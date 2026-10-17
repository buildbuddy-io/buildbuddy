package flagz_test

import (
	stdflag "flag"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagz"
	"github.com/stretchr/testify/require"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

var testFlag = flag.String("flagz_test.flag", "original", "A flag for testing flagz.")

func TestMain(m *testing.M) {
	// The stdlib testing flags can't be rendered to YAML.
	stdflag.VisitAll(func(f *stdflag.Flag) {
		if strings.HasPrefix(f.Name, "test.") {
			flagyaml.IgnoreFlagForYAML(f.Name)
		}
	})
	os.Exit(m.Run())
}

func TestGetIgnoresQuery(t *testing.T) {
	rec := httptest.NewRecorder()
	flagz.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/flagz?flagz_test.flag=changed", nil))

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "flagz_test")
	require.Equal(t, "original", *testFlag)
}

func TestPostRejected(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/flagz", strings.NewReader("flagz_test.flag=changed"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	flagz.ServeHTTP(rec, req)

	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	require.Equal(t, "original", *testFlag)
}
