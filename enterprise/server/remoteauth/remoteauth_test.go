package remoteauth

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"
)

type fakeAuthService struct {
	nextJwt string
	nextErr error

	mu sync.Mutex
}

func (a *fakeAuthService) setNextJwt(t *testing.T, jwt string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	assert.NoError(t, a.nextErr)
	a.nextJwt = jwt
}

func (a *fakeAuthService) setNextErr(t *testing.T, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	assert.Equal(t, "", a.nextJwt)
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

func setup(t *testing.T) (interfaces.Authenticator, *fakeAuthService) {
	fakeAuthService := fakeAuthService{}
	te := testenv.GetTestEnv(t)
	grpcServer, runServer := testenv.RegisterLocalGRPCServer(te)
	authpb.RegisterAuthServiceServer(grpcServer, &fakeAuthService)
	go runServer()
	conn, err := testenv.LocalGRPCConn(context.Background(), te)
	assert.NoError(t, err)
	authenticator, err := newRemoteAuthenticator(conn)
	assert.NoError(t, err)
	return authenticator, &fakeAuthService
}

func contextWithApiKey(t *testing.T, key string) context.Context {
	ctx := metadata.AppendToOutgoingContext(context.Background(), authutil.APIKeyHeader, key)
	outgoingMD, ok := metadata.FromOutgoingContext(ctx)
	assert.True(t, ok)
	// Simulate an RPC by creating a new context with the incoming
	// metadata set to the previously applied outgoing metadata.
	ctx = context.Background()
	return metadata.NewIncomingContext(ctx, outgoingMD)
}

func TestAuthenticatedGRPCContext(t *testing.T) {
	authenticator, fakeAuth := setup(t)

	// No auth headers, fallback to remote.
	fakeAuth.setNextJwt(t, "nothing1")
	ctx := authenticator.AuthenticatedGRPCContext(context.Background())
	assert.Equal(t, "nothing1", ctx.Value(authutil.ContextTokenStringKey))

	// Don't cache responses for missing auth headers.
	fakeAuth.setNextJwt(t, "nothing2")
	ctx = authenticator.AuthenticatedGRPCContext(context.Background())
	assert.Equal(t, "nothing2", ctx.Value(authutil.ContextTokenStringKey))

	// Error case.
	fakeAuth.setNextErr(t, status.InternalError("error"))
	ctx = authenticator.AuthenticatedGRPCContext(context.Background())
	assert.Equal(t, nil, ctx.Value(authutil.ContextTokenStringKey))

	// Error case with API key.
	fakeAuth.setNextErr(t, status.InternalError("error"))
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	assert.Equal(t, nil, ctx.Value(authutil.ContextTokenStringKey))

	// Don't cache errors.
	fakeAuth.setNextJwt(t, "jwt")
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	assert.Equal(t, "jwt", ctx.Value(authutil.ContextTokenStringKey))

	// The next auth attempt should be cached.
	fakeAuth.setNextJwt(t, "twj")
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	assert.Equal(t, "jwt", ctx.Value(authutil.ContextTokenStringKey))

	// But a different API key should re-remotely-auth
	fakeAuth.setNextJwt(t, "twj")
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "bar"))
	assert.Equal(t, "twj", ctx.Value(authutil.ContextTokenStringKey))
}
