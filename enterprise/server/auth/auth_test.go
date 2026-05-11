package auth

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oidc"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestRegisterReturnsErrorWithNoAuthProviders(t *testing.T) {
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{})
	flags.Set(t, "auth.oauth_providers", []oidc.OauthProvider{})
	flags.Set(t, "auth.enable_self_auth", false)
	flags.Set(t, "auth.saml.cert", "")
	flags.Set(t, "auth.saml.cert_file", "")
	flags.Set(t, "github.jwt_key", "")

	err := Register(context.Background(), env)

	require.Error(t, err)
	require.NoError(t, RegisterNullAuth(env))
	require.IsType(t, &nullauth.NullAuthenticator{}, env.GetAuthenticator())
}
