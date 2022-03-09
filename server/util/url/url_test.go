package url_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/url"
)

func envWithAppURL(t *testing.T, appUrl string) environment.Env {
	config.RegisterAndParseFlags()
	c, err := config.NewConfiguratorFromData([]byte(fmt.Sprintf("app:\n  build_buddy_url: %s\n", appUrl)))
	if err != nil {
		t.Fatal(err)
	}
	c.ReconcileFlagsAndConfig()
	healthChecker := healthcheck.NewHealthChecker("test")
	return real_environment.NewRealEnv(c, healthChecker)
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
		err := url.ValidateRedirect(te, tc.RedirURL)
		gotErr := err != nil
		if gotErr != tc.WantErr {
			t.Fatalf("ValidateRedir(%q, %q) returned %s, wantErr: %t", tc.AppURL, tc.RedirURL, err, tc.WantErr)
		}
	}
}
