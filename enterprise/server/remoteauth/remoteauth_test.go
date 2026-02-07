package remoteauth

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgrpc"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testkeys"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"

	"github.com/golang-jwt/jwt/v4"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/pkg/openfeature/memprovider"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

type fakeAuthService struct {
	lastAuthRequest *authpb.AuthenticateRequest

	nextJwt map[string]string
	nextErr map[string]error

	publicKeys      []string
	publicKeysErr   error
	publicKeysCalls int

	mu sync.Mutex
}

func (a *fakeAuthService) Reset() *fakeAuthService {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastAuthRequest = nil
	a.nextJwt = map[string]string{}
	a.nextErr = map[string]error{}
	a.publicKeys = nil
	a.publicKeysErr = nil
	a.publicKeysCalls = 0
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
	a.lastAuthRequest = req
	if a.nextErr[req.GetSubdomain()] != nil {
		err := a.nextErr[req.GetSubdomain()]
		a.nextErr[req.GetSubdomain()] = nil
		return nil, err
	}
	jwt := a.nextJwt[req.GetSubdomain()]
	a.nextJwt[req.GetSubdomain()] = ""
	return &authpb.AuthenticateResponse{Jwt: &jwt}, nil
}

func (a *fakeAuthService) setPublicKeys(keys []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.publicKeys = keys
	a.publicKeysErr = nil
}

func (a *fakeAuthService) setPublicKeysErr(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.publicKeys = nil
	a.publicKeysErr = err
}

func (a *fakeAuthService) getLastAuthRequest() *authpb.AuthenticateRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastAuthRequest
}

func (a *fakeAuthService) GetPublicKeys(ctx context.Context, req *authpb.GetPublicKeysRequest) (*authpb.GetPublicKeysResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.publicKeysCalls++
	if a.publicKeysErr != nil {
		return nil, a.publicKeysErr
	}
	resp := &authpb.GetPublicKeysResponse{}
	for _, key := range a.publicKeys {
		resp.PublicKeys = append(resp.PublicKeys, &authpb.PublicKey{Key: &key})
	}
	return resp, nil
}

func (a *fakeAuthService) getPublicKeysCalls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.publicKeysCalls
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
	conn, err := testenv.LocalGRPCConn(t.Context(), lis)
	require.NoError(t, err)
	authenticator, err := NewWithTarget(te, conn)
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
	ctx := metadata.AppendToOutgoingContext(t.Context(), key, value)
	return testgrpc.OutgoingToIncomingContext(t, ctx)
}

func validJwt(t *testing.T, uid string) string {
	authctx := claims.AuthContextWithJWT(t.Context(), &claims.Claims{UserID: uid}, nil)
	jwt, ok := authctx.Value(authutil.ContextTokenStringKey).(string)
	require.True(t, ok)
	require.NotEqual(t, "", jwt)
	return jwt
}

func validES256Jwt(t *testing.T, uid string) string {
	c := &claims.Claims{UserID: uid}
	tokenString, err := claims.AssembleJWT(c, jwt.SigningMethodES256)
	require.NoError(t, err)
	require.NotEqual(t, "", tokenString)
	return tokenString
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
	ctx := authenticator.AuthenticatedGRPCContext(t.Context())
	require.Equal(t, nil, ctx.Value(authutil.ContextTokenStringKey))

	// Don't cache responses for missing auth headers.
	fakeAuth.Reset().setNextJwt(t, "", validJwt(t, "nothing"))
	ctx = authenticator.AuthenticatedGRPCContext(t.Context())
	require.Equal(t, nil, ctx.Value(authutil.ContextTokenStringKey))

	// Error case.
	fakeAuth.Reset().setNextErr(t, "", status.InternalError("error"))
	ctx = authenticator.AuthenticatedGRPCContext(t.Context())
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

func setupExperimentProvider(t *testing.T, flagValue bool) interfaces.ExperimentFlagProvider {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"auth.remote.use_es256_jwts": {
			State:          memprovider.Enabled,
			DefaultVariant: "default",
			Variants: map[string]any{
				"default": flagValue,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	return fp
}

func TestUseES256SignedJWTs(t *testing.T) {
	ctx := t.Context()

	// Test with nil experiment provider and flag disabled (default)
	flags.Set(t, "auth.remote.use_es256_jwts", false)
	require.False(t, useES256SignedJWTs(ctx, nil))

	// Test with nil experiment provider and flag enabled
	flags.Set(t, "auth.remote.use_es256_jwts", true)
	require.True(t, useES256SignedJWTs(ctx, nil))

	// Test with experiment provider returning false and flag disabled
	flags.Set(t, "auth.remote.use_es256_jwts", false)
	provider := setupExperimentProvider(t, false)
	require.False(t, useES256SignedJWTs(ctx, provider))

	// Test with experiment provider returning true and flag disabled
	flags.Set(t, "auth.remote.use_es256_jwts", false)
	provider = setupExperimentProvider(t, true)
	require.True(t, useES256SignedJWTs(ctx, provider))

	// Test with experiment provider returning false and flag enabled (flag takes precedence)
	flags.Set(t, "auth.remote.use_es256_jwts", true)
	provider = setupExperimentProvider(t, false)
	require.True(t, useES256SignedJWTs(ctx, provider))
}

func TestAuthenticateRequestsES256WhenEnabled(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	require.NoError(t, claims.Init())

	flags.Set(t, "auth.remote.use_es256_jwts", true)
	authenticator, fakeAuth := setup(t)

	fooJwt := validES256Jwt(t, "foo")
	fakeAuth.setPublicKeys([]string{keyPair.PublicKeyPEM})
	fakeAuth.setNextJwt(t, "", fooJwt)
	ctx := contextWithApiKey(t, "foo")
	ctx = authenticator.AuthenticatedGRPCContext(ctx)
	user, err := authenticator.AuthenticatedUser(ctx)
	require.NoError(t, err)
	require.Equal(t, "foo", user.GetUserID())

	req := fakeAuth.getLastAuthRequest()
	require.NotNil(t, req)
	require.Equal(t, authpb.JWTSigningMethod_ES256, req.GetJwtSigningMethod())
}

func TestAuthenticateDoesNotRequestES256WhenDisabled(t *testing.T) {
	// Disable ES256 flag
	flags.Set(t, "auth.remote.use_es256_jwts", false)
	authenticator, fakeAuth := setup(t)

	fooJwt := validJwt(t, "foo")
	fakeAuth.Reset().setNextJwt(t, "", fooJwt)
	ctx := contextWithApiKey(t, "foo")
	authenticator.AuthenticatedGRPCContext(ctx)

	// Verify the request did not include ES256 signing method
	req := fakeAuth.getLastAuthRequest()
	require.NotNil(t, req)
	require.Equal(t, authpb.JWTSigningMethod_UNKNOWN, req.GetJwtSigningMethod())
}

func TestES256KeysCached(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	require.NoError(t, claims.Init())

	flags.Set(t, "auth.remote.use_es256_jwts", true)
	authenticator, fakeAuth := setup(t)

	fakeAuth.setPublicKeys([]string{keyPair.PublicKeyPEM})

	// First authentication should fetch keys.
	fakeAuth.setNextJwt(t, "", validES256Jwt(t, "foo"))
	ctx := authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	user, err := authenticator.AuthenticatedUser(ctx)
	require.NoError(t, err)
	require.Equal(t, "foo", user.GetUserID())
	require.Equal(t, 1, fakeAuth.getPublicKeysCalls())

	// Second authentication with a different API key should use cached keys.
	fakeAuth.setNextJwt(t, "", validES256Jwt(t, "bar"))
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "bar"))
	user, err = authenticator.AuthenticatedUser(ctx)
	require.NoError(t, err)
	require.Equal(t, "bar", user.GetUserID())
	require.Equal(t, 1, fakeAuth.getPublicKeysCalls())
}

func TestES256KeyCacheExpiry(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	require.NoError(t, claims.Init())

	flags.Set(t, "auth.remote.use_es256_jwts", true)
	flags.Set(t, "auth.remote.es256_key_cache_ttl", time.Nanosecond)
	authenticator, fakeAuth := setup(t)

	fakeAuth.setPublicKeys([]string{keyPair.PublicKeyPEM})

	// First authentication fetches keys.
	fakeAuth.setNextJwt(t, "", validES256Jwt(t, "foo"))
	ctx := authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	user, err := authenticator.AuthenticatedUser(ctx)
	require.NoError(t, err)
	require.Equal(t, "foo", user.GetUserID())
	require.Equal(t, 1, fakeAuth.getPublicKeysCalls())

	// Second authentication should re-fetch because the TTL has expired.
	fakeAuth.setNextJwt(t, "", validES256Jwt(t, "bar"))
	ctx = authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "bar"))
	user, err = authenticator.AuthenticatedUser(ctx)
	require.NoError(t, err)
	require.Equal(t, "bar", user.GetUserID())
	require.Greater(t, fakeAuth.getPublicKeysCalls(), 1)
}

func TestES256KeyCacheInvalidationOnKeyRotation(t *testing.T) {
	keyPairA := testkeys.GenerateES256KeyPair(t)
	keyPairB := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPairA.PrivateKeyPEM)
	require.NoError(t, claims.Init())

	flags.Set(t, "auth.remote.use_es256_jwts", true)
	authenticator, fakeAuth := setup(t)

	// Start with key A.
	fakeAuth.setPublicKeys([]string{keyPairA.PublicKeyPEM})
	fakeAuth.setNextJwt(t, "", validES256Jwt(t, "foo"))
	ctx := authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	user, err := authenticator.AuthenticatedUser(ctx)
	require.NoError(t, err)
	require.Equal(t, "foo", user.GetUserID())
	require.Equal(t, 1, fakeAuth.getPublicKeysCalls())

	// Rotate to key B on the server. The cache still has key A.
	fakeAuth.setPublicKeys([]string{keyPairB.PublicKeyPEM})

	// Simulate receiving a JWT signed with key B (e.g. from the backend
	// after rotation). Switch the signing key so validES256Jwt uses key B.
	flags.Set(t, "auth.jwt_es256_private_key", keyPairB.PrivateKeyPEM)
	require.NoError(t, claims.Init())

	// Pass the JWT signed with key B directly via metadata. Verification
	// should fail with cached key A, trigger invalidation, re-fetch key B,
	// and succeed on retry.
	jwtB := validES256Jwt(t, "bar")
	ctx = authenticator.AuthenticatedGRPCContext(contextWithJwt(t, jwtB))
	user, err = authenticator.AuthenticatedUser(ctx)
	require.NoError(t, err)
	require.Equal(t, "bar", user.GetUserID())
	// Should have fetched keys a second time due to invalidation.
	require.Equal(t, 2, fakeAuth.getPublicKeysCalls())
}

func TestES256KeyCacheNotInvalidatedOnNonSignatureError(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	require.NoError(t, claims.Init())

	flags.Set(t, "auth.remote.use_es256_jwts", true)
	// Use a very large expiration buffer so that the JWT is considered
	// "expiring too soon", which is a non-signature validation error.
	flags.Set(t, "auth.remote.jwt_expiration_buffer", 24*time.Hour)
	authenticator, fakeAuth := setup(t)

	fakeAuth.setPublicKeys([]string{keyPair.PublicKeyPEM})

	// First auth to populate the key cache.
	fakeAuth.setNextJwt(t, "", validES256Jwt(t, "foo"))
	authenticator.AuthenticatedGRPCContext(contextWithApiKey(t, "foo"))
	// Auth should fail because the JWT expires too soon, but this is
	// expected — we only care about key fetch counts.
	require.Equal(t, 1, fakeAuth.getPublicKeysCalls())

	// Pass an ES256 JWT directly via metadata. The signature is valid but
	// the JWT will be rejected because it expires within the buffer. This
	// non-signature error should NOT trigger key invalidation.
	jwtStr := validES256Jwt(t, "bar")
	ctx := authenticator.AuthenticatedGRPCContext(contextWithJwt(t, jwtStr))
	_, err := authenticator.AuthenticatedUser(ctx)
	require.Error(t, err)
	// Keys should NOT have been re-fetched because the error was not a
	// signature error.
	require.Equal(t, 1, fakeAuth.getPublicKeysCalls())
}
