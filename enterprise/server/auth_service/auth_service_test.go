package auth_service

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

func contextWithApiKey(t *testing.T, key string) context.Context {
	ctx := metadata.AppendToOutgoingContext(context.Background(), authutil.APIKeyHeader, key)
	outgoingMD, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	// Simulate an RPC by creating a new context with the incoming
	// metadata set to the previously applied outgoing metadata.
	ctx = context.Background()
	return metadata.NewIncomingContext(ctx, outgoingMD)
}

func TestAuthenticateNoCreds(t *testing.T) {
	service := AuthService{authenticator: testauth.NewTestAuthenticator(testauth.TestUsers("foo", "bar"))}
	_, err := service.Authenticate(context.Background(), &authpb.AuthenticateRequest{})
	require.True(t, status.IsUnauthenticatedError(err))
}

func TestAuthenticate(t *testing.T) {
	service := AuthService{authenticator: testauth.NewTestAuthenticator(testauth.TestUsers("foo", "bar"))}
	resp, err := service.Authenticate(contextWithApiKey(t, "foo"), &authpb.AuthenticateRequest{})
	require.NoError(t, err)
	require.NotEqual(t, 0, len(*resp.Jwt))
}

func TestAuthenticateWrongCreds(t *testing.T) {
	service := AuthService{authenticator: testauth.NewTestAuthenticator(testauth.TestUsers("foo", "bar"))}
	_, err := service.Authenticate(contextWithApiKey(t, "baz"), &authpb.AuthenticateRequest{})
	require.True(t, status.IsUnauthenticatedError(err))
}

func TestGetPublicKeys_OneKey(t *testing.T) {
	flags.Set(t, "auth.jwt_rsa_public_key", "thekey")
	env := testenv.GetTestEnv(t)
	Register(env)
	service := env.GetAuthService()
	resp, err := service.GetPublicKeys(context.Background(), &authpb.GetPublicKeysRequest{})
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.GetPublicKeys()))
	require.Equal(t, "thekey", resp.GetPublicKeys()[0].GetKey())
}

func TestGetPublicKeys_MultipleKeys(t *testing.T) {
	flags.Set(t, "auth.jwt_rsa_public_key", "thekey")
	flags.Set(t, "auth.new_jwt_rsa_public_key", "newandimproved")
	env := testenv.GetTestEnv(t)
	Register(env)
	service := env.GetAuthService()
	resp, err := service.GetPublicKeys(context.Background(), &authpb.GetPublicKeysRequest{})
	require.NoError(t, err)
	require.Equal(t, 2, len(resp.GetPublicKeys()))
	require.Equal(t, "newandimproved", resp.GetPublicKeys()[0].GetKey())
	require.Equal(t, "thekey", resp.GetPublicKeys()[1].GetKey())
}
