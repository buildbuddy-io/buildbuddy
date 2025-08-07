package auth_service

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgrpc"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

func contextWithApiKey(t *testing.T, key string) context.Context {
	ctx := metadata.AppendToOutgoingContext(context.Background(), authutil.APIKeyHeader, key)
	return testgrpc.OutgoingToIncomingContext(t, ctx)
}

func TestAuthenticateNoCreds(t *testing.T) {
	service := AuthService{authenticator: testauth.NewTestAuthenticator(testauth.TestUsers("foo", "bar"))}
	_, err := service.Authenticate(context.Background(), &authpb.AuthenticateRequest{})
	assert.True(t, status.IsUnauthenticatedError(err))
}

func TestAuthenticate(t *testing.T) {
	service := AuthService{authenticator: testauth.NewTestAuthenticator(testauth.TestUsers("foo", "bar"))}
	resp, err := service.Authenticate(contextWithApiKey(t, "foo"), &authpb.AuthenticateRequest{})
	assert.NoError(t, err)
	assert.NotEqual(t, 0, len(*resp.Jwt))
}

func TestAuthenticateWrongCreds(t *testing.T) {
	service := AuthService{authenticator: testauth.NewTestAuthenticator(testauth.TestUsers("foo", "bar"))}
	_, err := service.Authenticate(contextWithApiKey(t, "baz"), &authpb.AuthenticateRequest{})
	assert.True(t, status.IsUnauthenticatedError(err))
}
