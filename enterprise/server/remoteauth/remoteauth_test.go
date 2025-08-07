package remoteauth

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgrpc"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"google.golang.org/grpc/metadata"
)

type fakeAuthService struct {
	nextJwt map[string]string
	nextErr map[string]error

	mu sync.Mutex
}

func (a *fakeAuthService) Reset() *fakeAuthService {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nextErr = map[string]error{}
	a.nextJwt = map[string]string{}
	return a
}

func (a *fakeAuthService) setNextJwt(t *testing.T, sub, jwt string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NoError(t, a.nextErr[sub])
	a.nextJwt[sub] = jwt
}

func (a *fakeAuthService) setNextErr(t *testing.T, sub string, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.Equal(t, "", a.nextJwt[sub])
	a.nextErr[sub] = err
}

func (a *fakeAuthService) Authenticate(ctx context.Context, req *authpb.AuthenticateRequest) (*authpb.AuthenticateResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.nextErr[req.GetSubdomain()] != nil {
		err := a.nextErr[req.GetSubdomain()]
		a.nextErr[req.GetSubdomain()] = nil
		return nil, err
	}
	jwt := a.nextJwt[req.GetSubdomain()]
	a.nextJwt[req.GetSubdomain()] = ""
	return &authpb.AuthenticateResponse{Jwt: &jwt}, nil
}

func (a *fakeAuthService) GetPublicKeys(ctx context.Context, req *authpb.GetPublicKeysRequest) (*authpb.GetPublicKeysResponse, error) {
	return &authpb.GetPublicKeysResponse{}, status.UnimplementedError("GetPublicKeys unimplemented")
}

func setup(t *testing.T) (interfaces.Authenticator, *fakeAuthService) {
	fakeAuthService := fakeAuthService{
		nextErr: map[string]error{},
		nextJwt: map[string]string{},
	}
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
	return testgrpc.OutgoingToIncomingContext(t, ctx)
}

func validJwt(t *testing.T, uid string) string {
	authctx := claims.AuthContextWithJWT(context.Background(), &claims.Claims{UserID: uid}, nil)
	jwt, ok := authctx.Value(authutil.ContextTokenStringKey).(string)
	require.True(t, ok)
	require.NotEqual(t, "", jwt)
	return jwt
}

func TestAuthenticatedGRPCContext(t *testing.T) {
	authenticator, fakeAuth := setup(t)

	fooJwt := validJwt(t, "foo")
	barJwt := validJwt(t, "bar")
	bazJwt := validJwt(t, "baz")
	require.NotEqual(t, fooJwt, barJwt)
	require.NotEqual(t, fooJwt, bazJwt)
	require.NotEqual(t, barJwt, bazJwt)

	// Fail if there are no auth headers.
	fakeAuth.setNextJwt(t, "", validJwt(t, "nothing"))
	ctx := authenticator.AuthenticatedGRPCContext(context.Background())
	require.Equal(t, nil, ctx.Value(authutil.ContextTokenStringKey))

	// Don't cache responses for missing auth headers.
	fakeAuth.Reset().setNextJwt(t, "", validJwt(t, "nothing"))
	ctx = authenticator.AuthenticatedGRPCContext(context.Background())
	require.Equal(t, nil, ctx.Value(authutil.ContextTokenStringKey))

	// Error case.
	fakeAuth.Reset().setNextErr(t, "", status.InternalError("error"))
	ctx = authenticator.AuthenticatedGRPCContext(context.Background())
	require.Nil(t, ctx.Value(authutil.ContextTokenStringKey))

	// Error case with API Key.
	fakeAuth.Reset().setNextErr(t, "", status.InternalError("error"))
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	require.Nil(t, ctx.Value(authutil.ContextTokenStringKey))
	err, _ := authutil.AuthErrorFromContext(ctx)
	require.True(t, status.IsInternalError(err))

	// Don't cache errors.
	fakeAuth.Reset().setNextJwt(t, "", fooJwt)
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	require.Equal(t, fooJwt, ctx.Value(authutil.ContextTokenStringKey))

	// The next auth attempt should be cached.
	fakeAuth.Reset().setNextJwt(t, "", barJwt)
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	require.Equal(t, fooJwt, ctx.Value(authutil.ContextTokenStringKey))

	// But a different API Key should re-remotely-auth
	fakeAuth.Reset().setNextJwt(t, "", barJwt)
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "bar"))
	require.Equal(t, barJwt, ctx.Value(authutil.ContextTokenStringKey))

	// Valid JWTs should be passed through
	fakeAuth.Reset().setNextJwt(t, "", bazJwt)
	ctx = authenticator.AuthenticatedGRPCContext(contextWithJwt(t, bazJwt))
	require.Equal(t, bazJwt, ctx.Value(authutil.ContextTokenStringKey))

	// Invalid JWTs should return an error
	fakeAuth.Reset().setNextJwt(t, "", "invalid")
	ctx = authenticator.AuthenticatedGRPCContext(contextWithJwt(t, "baz"))
	require.Nil(t, ctx.Value(authutil.ContextTokenStringKey))
	err, _ = authutil.AuthErrorFromContext(ctx)
	require.NotNil(t, err)
}

func TestJwtExpiry(t *testing.T) {
	flags.Set(t, "auth.jwt_duration", 50*time.Second)
	authenticator, fakeAuth := setup(t)

	fooJwt := validJwt(t, "foo")
	barJwt := validJwt(t, "bar")
	require.NotEqual(t, fooJwt, barJwt)

	// The JWT minted by the backend should be considered to expire too soon
	// and should not be used.
	fakeAuth.Reset().setNextJwt(t, "", fooJwt)
	ctx := authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	require.Nil(t, ctx.Value(authutil.ContextTokenStringKey))
}

func TestSubdomains(t *testing.T) {
	authenticator, fakeAuth := setup(t)

	fooJwt := validJwt(t, "foo")
	barJwt := validJwt(t, "bar")
	bazJwt := validJwt(t, "baz")
	require.NotEqual(t, fooJwt, barJwt)
	require.NotEqual(t, fooJwt, bazJwt)
	require.NotEqual(t, barJwt, bazJwt)

	// Authenticate at foosub.buildbuddy.io with API key foo
	fakeAuth.Reset().setNextJwt(t, "foosub", fooJwt)
	ctx := subdomain.Context(contextWithApiKey(t, "foo"), "foosub")
	ctx = authenticator.AuthenticatedGRPCContext(ctx)
	require.Equal(t, fooJwt, ctx.Value(authutil.ContextTokenStringKey))

	// Ensure that JWT is not cached for a different subdomain
	fakeAuth.Reset().setNextJwt(t, "barsub", barJwt)
	ctx = subdomain.Context(contextWithApiKey(t, "foo"), "barsub")
	ctx = authenticator.AuthenticatedGRPCContext(ctx)
	require.Equal(t, barJwt, ctx.Value(authutil.ContextTokenStringKey))

	// Also ensure API key bar doesn't work for foosub.buildbuddy.io
	fakeAuth.Reset().setNextJwt(t, "foosub", bazJwt)
	ctx = subdomain.Context(contextWithApiKey(t, "bar"), "foosub")
	ctx = authenticator.AuthenticatedGRPCContext(ctx)
	require.Equal(t, bazJwt, ctx.Value(authutil.ContextTokenStringKey))
}
