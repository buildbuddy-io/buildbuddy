package remoteauth

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"
)

type fakeAuthService struct {
	nextJwt string
	nextErr error

	mu sync.Mutex
}

func (a *fakeAuthService) Reset() *fakeAuthService {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nextErr = nil
	a.nextJwt = ""
	return a
}

func (a *fakeAuthService) setNextJwt(t *testing.T, jwt string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NoError(t, a.nextErr)
	a.nextJwt = jwt
}

func (a *fakeAuthService) setNextErr(t *testing.T, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.Equal(t, "", a.nextJwt)
	a.nextErr = err
}

func (a *fakeAuthService) Authenticate(ctx context.Context, req *authpb.AuthenticateRequest) (*authpb.AuthenticateResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.nextErr != nil {
		err := a.nextErr
		a.nextErr = nil
		return nil, err
	}
	jwt := a.nextJwt
	a.nextJwt = ""
	return &authpb.AuthenticateResponse{Jwt: &jwt}, nil
}

func (a *fakeAuthService) GetPublicKeys(ctx context.Context, req *authpb.GetPublicKeysRequest) (*authpb.GetPublicKeysResponse, error) {
	return &authpb.GetPublicKeysResponse{}, status.UnimplementedError("GetPublicKeys unimplemented")
}

func setup(t *testing.T) (interfaces.Authenticator, *fakeAuthService) {
	fakeAuthService := fakeAuthService{}
	te := testenv.GetTestEnv(t)
	grpcServer, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	authpb.RegisterAuthServiceServer(grpcServer, &fakeAuthService)
	go runServer()
	conn, err := testenv.LocalGRPCConn(context.Background(), lis)
	require.NoError(t, err)
	authenticator, err := newRemoteAuthenticator(conn)
	require.NoError(t, err)
	return authenticator, &fakeAuthService
}

func contextWithApiKey(t *testing.T, key string) context.Context {
	return contextWith(t, authutil.APIKeyHeader, key)
}

func contextWithJwt(t *testing.T, jwt string) context.Context {
	return contextWith(t, authutil.ContextTokenStringKey, jwt)
}

func contextWith(t *testing.T, key string, value string) context.Context {
	ctx := metadata.AppendToOutgoingContext(context.Background(), key, value)
	outgoingMD, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	// Simulate an RPC by creating a new context with the incoming
	// metadata set to the previously applied outgoing metadata.
	ctx = context.Background()
	return metadata.NewIncomingContext(ctx, outgoingMD)
}

func validJwt(t *testing.T) string {
	authctx := claims.AuthContextFromClaims(context.Background(), &claims.Claims{}, nil)
	jwt, ok := authctx.Value(authutil.ContextTokenStringKey).(string)
	require.True(t, ok)
	require.NotEqual(t, "", jwt)
	return jwt
}

func TestAuthenticatedGRPCContext(t *testing.T) {
	authenticator, fakeAuth := setup(t)

	// Fail if there are no auth headers.
	fakeAuth.setNextJwt(t, "nothing")
	ctx := authenticator.AuthenticatedGRPCContext(context.Background())
	require.Equal(t, nil, ctx.Value(authutil.ContextTokenStringKey))

	// Don't cache responses for missing auth headers.
	fakeAuth.Reset().setNextJwt(t, "nothing")
	ctx = authenticator.AuthenticatedGRPCContext(context.Background())
	require.Equal(t, nil, ctx.Value(authutil.ContextTokenStringKey))

	// Error case.
	fakeAuth.Reset().setNextErr(t, status.InternalError("error"))
	ctx = authenticator.AuthenticatedGRPCContext(context.Background())
	require.Nil(t, ctx.Value(authutil.ContextTokenStringKey))

	// Error case with API Key.
	fakeAuth.Reset().setNextErr(t, status.InternalError("error"))
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	require.Nil(t, ctx.Value(authutil.ContextTokenStringKey))
	err, _ := authutil.AuthErrorFromContext(ctx)
	require.True(t, status.IsInternalError(err))

	// Don't cache errors.
	fakeAuth.Reset().setNextJwt(t, "jwt")
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	require.Equal(t, "jwt", ctx.Value(authutil.ContextTokenStringKey))

	// The next auth attempt should be cached.
	fakeAuth.Reset().setNextJwt(t, "twj")
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	require.Equal(t, "jwt", ctx.Value(authutil.ContextTokenStringKey))

	// But a different API Key should re-remotely-auth
	fakeAuth.Reset().setNextJwt(t, "twj")
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "bar"))
	require.Equal(t, "twj", ctx.Value(authutil.ContextTokenStringKey))

	// Valid JWTs should be passed through
	jwt := validJwt(t)
	fakeAuth.Reset().setNextJwt(t, "jwt")
	ctx = authenticator.AuthenticatedGRPCContext(contextWithJwt(t, jwt))
	require.Equal(t, jwt, ctx.Value(authutil.ContextTokenStringKey))

	// Invalid JWTs should return an error
	fakeAuth.Reset().setNextJwt(t, "jwt")
	ctx = authenticator.AuthenticatedGRPCContext(contextWithJwt(t, "baz"))
	require.Nil(t, ctx.Value(authutil.ContextTokenStringKey))
	err, _ = authutil.AuthErrorFromContext(ctx)
	require.NotNil(t, err)
}
