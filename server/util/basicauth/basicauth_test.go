package basicauth_test

import (
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/basicauth"
	"github.com/stretchr/testify/require"
)

func TestBasicAuth(t *testing.T) {
	creds := map[string]string{
		"":                     "pass-with-empty-user",
		"user-with-empty-pass": "",
		"good":                 "1234",
	}
	auth := basicauth.Middleware(basicauth.DefaultRealm, creds)
	addr := testhttp.StartServer(t, auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK\n"))
	})))

	for _, testCase := range []struct {
		User, Pass     string
		ExpectedStatus int
		ExpectedBody   string
	}{
		{"evil", "4567", 401, "Unauthorized\n"},
		{"evil", "1234", 401, "Unauthorized\n"},
		{"good", "4567", 401, "Unauthorized\n"},
		{"", "1234", 401, "Unauthorized\n"},
		{"good", "", 401, "Unauthorized\n"},
		{"good", "1234", 200, "OK\n"},
		{"", "pass-with-empty-user", 200, "OK\n"},
		{"user-with-empty-pass", "", 200, "OK\n"},
	} {
		authURL := *addr
		authURL.User = url.UserPassword(testCase.User, testCase.Pass)

		res, err := http.Get(authURL.String())

		require.NoError(t, err)
		require.Equal(t, testCase.ExpectedStatus, res.StatusCode)
		b, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.Equal(t, testCase.ExpectedStatus, res.StatusCode)
		require.Equal(t, testCase.ExpectedBody, string(b))
	}
}
