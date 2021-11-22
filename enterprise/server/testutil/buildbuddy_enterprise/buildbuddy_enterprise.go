package buildbuddy_enterprise

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
)

const (
	DefaultConfig = "enterprise/config/test/buildbuddy.fakeauth.yaml"
	NoAuthConfig  = "enterprise/config/test/buildbuddy.noauth.yaml"
)

func Run(t *testing.T, args ...string) *app.App {
	return RunWithConfig(t, DefaultConfig, args...)
}

func RunWithConfig(t *testing.T, configPath string, args ...string) *app.App {
	redisTarget := testredis.Start(t)
	commandArgs := []string{
		fmt.Sprintf("--telemetry_port=%d", app.FreePort(t)),
		"--app_directory=/enterprise/app",
		"--app.default_redis_target=" + redisTarget,
		"--auth.enable_self_auth=true",
		"--auth.jwt_key=BuIlDbUdDy",
	}
	commandArgs = append(commandArgs, args...)
	return app.Run(
		t,
		/* commandPath= */ "enterprise/server/cmd/server/buildbuddy_/buildbuddy",
		commandArgs,
		/* configPath= */ configPath,
	)
}

// login logs in as the default user using the self-auth flow, and returns the
// JWT.
func login(t *testing.T, a *app.App) string {
	jar, err := cookiejar.New(nil /*=options*/)
	require.NoError(t, err)
	client := &http.Client{Jar: jar}

	escapedAppURL := url.QueryEscape(a.HTTPURL())
	loginURL := fmt.Sprintf(
		"%s/login/?issuer_url=%s&redirect_url=%s",
		a.HTTPURL(), escapedAppURL, escapedAppURL)

	res, err := client.Get(loginURL)
	require.NoError(t, err)
	io.ReadAll(res.Body)
	res.Body.Close()

	u, err := url.Parse(a.HTTPURL())
	require.NoError(t, err)
	cookies := jar.Cookies(u)
	for _, c := range cookies {
		if c.Name == "Authorization" {
			return c.Value
		}
	}

	require.FailNowf(t, "missing 'Authorization' cookie", "all cookies: %v", cookies)
	return ""
}

// Authenticate logs into the app as the default self-auth user ("Build Buddy"),
// creates the user if it does not already exist in the DB, and returns a
// modified context with the resulting credentials, suitable for making
// authenticated RPCs to the app.
func WithDefaultSelfAuthUser(t *testing.T, ctx context.Context, a *app.App) context.Context {
	jwt := login(t, a)
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-jwt", jwt)
	client := a.BuildBuddyServiceClient(t)
	_, err := client.GetUser(ctx, &uspb.GetUserRequest{})
	if err == nil {
		// User already exists; no need to create one.
		return ctx
	}
	if !strings.Contains(err.Error(), "not found") {
		require.FailNowf(t, "GetUser failed", "%s", err)
		return nil
	}
	return ctx
}
