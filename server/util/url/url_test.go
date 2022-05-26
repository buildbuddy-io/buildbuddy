package url_test

import (
	"flag"
	"net/url"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	bburl "github.com/buildbuddy-io/buildbuddy/server/util/url"
)

func init() {
	flag.Parse()
}

func envWithAppURL(t *testing.T, appUrl string) environment.Env {
	u, err := url.Parse(appUrl)
	require.NoError(t, err)
	flags.Set(t, "app.build_buddy_url", *u)
	healthChecker := healthcheck.NewHealthChecker("test")
	return real_environment.NewRealEnv(healthChecker)
}

func TestValidateRedirect(t *testing.T) {
	testCases := []struct {
		AppURL   string
		RedirURL string
		WantErr  bool
	}{
		{
			AppURL:   "",
			RedirURL: "/foo",
			WantErr:  false,
		},
		{
			AppURL:   "",
			RedirURL: "http://evil.com/foo",
			WantErr:  true,
		},
		{
			AppURL:   "http://localhost:8080",
			RedirURL: "/foo",
			WantErr:  false,
		},
		{
			AppURL:   "http://localhost:8080",
			RedirURL: "http://evil.com/foo",
			WantErr:  true,
		},
		{
			AppURL:   "http://localhost:8080",
			RedirURL: "http://localhost:8080/auth",
			WantErr:  false,
		},
	}

	for _, tc := range testCases {
		te := envWithAppURL(t, tc.AppURL)
		err := bburl.ValidateRedirect(te, tc.RedirURL)
		gotErr := err != nil
		if gotErr != tc.WantErr {
			t.Fatalf("ValidateRedir(%q, %q) returned %s, wantErr: %t", tc.AppURL, tc.RedirURL, err, tc.WantErr)
		}
	}
}
