package remoteauth

import (
	"context"
	"flag"
	"net/http"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

const (
	// Remote authentication results are cached locally in an LRU keyed by
	// API Key. The LRU is limited to this many entries. If the cache grows
	// beyond this size, old entries will be evicted. Increasing the size of
	// the jwt cache will result in fewer evictions (if the cache fills up) at
	// the cost of more memory use.
	jwtCacheSize = 10_000
)

var (
	authHeaders = []string{authutil.APIKeyHeader}

	target                 = flag.String("auth.remote.target", "", "The gRPC target of the remote authentication API.")
	jwtExpirationBuffer    = flag.Duration("auth.remote.jwt_expiration_buffer", time.Minute, "Discard remote-auth minted JWTs if they're within this time buffer of their expiration time.")
	alwaysUseRSASignedJWTs = flag.Bool("auth.remote.use_rsa_jwts", false, "If true, the remote authenticator always uses RSA-signed JWTs.")
	rsaKeyRefreshPeriod    = flag.Duration("auth.remote.rsa_key_refresh_period", 5*time.Minute, "How often to refresh the RSA public keys cache. Set to 0 to disable caching.")
)

func Register(env *real_environment.RealEnv) error {
	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return err
	}
	authenticator, err := newRemoteAuthenticator(env.GetExperimentFlagProvider(), conn)
	if err != nil {
		return err
	}
	env.SetAuthenticator(authenticator)
	return nil
}

func newRemoteAuthenticator(
	experimentFlagProvider interfaces.ExperimentFlagProvider,
	conn grpc.ClientConnInterface) (*RemoteAuthenticator, error) {
	config := &lru.Config[string]{
		MaxSize: jwtCacheSize,
		SizeFn:  func(v string) int64 { return 1 },
	}
	cache, err := lru.NewLRU(config)
	if err != nil {
		return nil, err
	}
	client := authpb.NewAuthServiceClient(conn)
	provider := &keyProvider{
		experimentFlagProvider: experimentFlagProvider,
		client:                 client,
	}
	claimsParser, err := claims.NewClaimsParser(provider.provide)
	if err != nil {
		return nil, err
	}
	return &RemoteAuthenticator{
		authClient:             authpb.NewAuthServiceClient(conn),
		cache:                  cache,
		jwtExpirationBuffer:    *jwtExpirationBuffer,
		claimsParser:           claimsParser,
		experimentFlagProvider: experimentFlagProvider,
		keyProvider:            provider,
	}, nil
}

type keyProvider struct {
	experimentFlagProvider interfaces.ExperimentFlagProvider
	client                 authpb.AuthServiceClient

	mu              sync.RWMutex
	cachedRSAKeys   []string
	cacheExpiration time.Time
}

func (kp *keyProvider) provide(ctx context.Context) ([]string, error) {
	keys := []string{}
	if useRSASignedJWTs(ctx, kp.experimentFlagProvider) {
		rsaKeys, err := kp.getRSAPublicKeys(ctx)
		if err != nil {
			return []string{}, err
		}
		keys = append(keys, rsaKeys...)
	}
	defaultKeys, err := claims.DefaultKeyProvider(ctx)
	if err != nil {
		return []string{}, err
	}
	keys = append(keys, defaultKeys...)
	return keys, nil
}

func (kp *keyProvider) getRSAPublicKeys(ctx context.Context) ([]string, error) {
	refreshPeriod := *rsaKeyRefreshPeriod
	if refreshPeriod <= 0 {
		return kp.fetchRSAPublicKeys(ctx)
	}

	kp.mu.RLock()
	keys := kp.cachedRSAKeys
	expiration := kp.cacheExpiration
	kp.mu.RUnlock()

	if len(keys) > 0 && time.Now().Before(expiration) {
		return keys, nil
	}

	kp.mu.Lock()
	defer kp.mu.Unlock()

	// Double-check after acquiring write lock.
	if len(kp.cachedRSAKeys) > 0 && time.Now().Before(kp.cacheExpiration) {
		return kp.cachedRSAKeys, nil
	}

	keys, err := kp.fetchRSAPublicKeys(ctx)
	if err != nil {
		return nil, err
	}
	kp.cachedRSAKeys = keys
	kp.cacheExpiration = time.Now().Add(refreshPeriod)
	return keys, nil
}

func (kp *keyProvider) fetchRSAPublicKeys(ctx context.Context) ([]string, error) {
	req := authpb.GetPublicKeysRequest{}
	resp, err := kp.client.GetPublicKeys(ctx, &req)
	if err != nil {
		return []string{}, err
	}
	keys := make([]string, len(resp.GetPublicKeys()))
	for i, key := range resp.GetPublicKeys() {
		keys[i] = key.GetKey()
	}
	return keys, nil
}

func (kp *keyProvider) invalidateKeys() {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	kp.cachedRSAKeys = nil
	kp.cacheExpiration = time.Time{}
}

func useRSASignedJWTs(
	ctx context.Context,
	experimentFlagProvider interfaces.ExperimentFlagProvider) bool {
	return *alwaysUseRSASignedJWTs ||
		experimentFlagProvider.Boolean(ctx, "auth.remote.use_rsa_jwts", false)
}

// The claims cache must be keyed by subdomain and API key to avoid keys
// leaking across subdomain boundaries. This function extracts an API key and
// subdomain from a context and builds a claims cache key with them.
func claimsCacheKey(ctx context.Context) (string, error) {
	key := getLastMetadataValue(ctx, authutil.APIKeyHeader)
	if key == "" {
		return "", status.PermissionDeniedError("Missing API key")
	}
	sub := subdomain.Get(ctx)
	return sub + ":" + key, nil
}

type RemoteAuthenticator struct {
	authClient             authpb.AuthServiceClient
	cache                  interfaces.LRU[string]
	jwtExpirationBuffer    time.Duration
	mu                     sync.RWMutex // protects cache
	claimsParser           *claims.ClaimsParser
	experimentFlagProvider interfaces.ExperimentFlagProvider
	keyProvider            *keyProvider
}

// Unsupported in the remote authenticator.
func (a *RemoteAuthenticator) AdminGroupID() string {
	return ""
}

// Unsupported in the remote authenticator.
func (a *RemoteAuthenticator) AnonymousUsageEnabled(ctx context.Context) bool {
	return false
}

// Unsupported in remote authenticator.
func (a *RemoteAuthenticator) PublicIssuers() []string {
	return []string{}
}

func (a *RemoteAuthenticator) Login(w http.ResponseWriter, r *http.Request) error {
	return status.UnimplementedError("HTTP login unsupported with remote authentication")
}

func (a *RemoteAuthenticator) Logout(w http.ResponseWriter, r *http.Request) error {
	return status.UnimplementedError("HTTP logout unsupported with remote authentication")
}

func (a *RemoteAuthenticator) Auth(w http.ResponseWriter, r *http.Request) error {
	return status.UnimplementedError("HTTP auth unsupported with remote authentication")
}

func (a *RemoteAuthenticator) AuthenticatedHTTPContext(w http.ResponseWriter, r *http.Request) context.Context {
	return r.Context()
}

func (a *RemoteAuthenticator) SSOEnabled() bool {
	return false
}

func (a *RemoteAuthenticator) AuthenticatedGRPCContext(ctx context.Context) context.Context {
	// If a JWT was provided, check if it's valid and use it if so.
	jwt, c, err := a.getValidJwtFromContext(ctx)
	if err != nil {
		return authutil.AuthContextWithError(ctx, err)
	}
	if jwt != "" {
		return authContext(ctx, jwt, c)
	}

	key, err := claimsCacheKey(ctx)
	if err != nil {
		return authutil.AuthContextWithError(ctx, err)
	}

	// Try to use a locally-cached JWT, if available and valid.
	a.mu.RLock()
	jwt, found := a.cache.Get(key)
	a.mu.RUnlock()
	if found {
		if c, err := a.jwtIsValid(ctx, jwt); err == nil {
			return authContext(ctx, jwt, c)
		}
		a.mu.Lock()
		a.cache.Remove(key)
		a.mu.Unlock()
	}

	// Otherwise, fetch a JWT from the remote auth service.
	jwt, err = a.authenticate(ctx)
	if err != nil {
		log.Debugf("Error remotely authenticating: %s", err)
		return authutil.AuthContextWithError(ctx, err)
	}
	a.mu.Lock()
	a.cache.Add(key, jwt)
	a.mu.Unlock()
	c, err = a.jwtIsValid(ctx, jwt)
	if err != nil {
		return authutil.AuthContextWithError(ctx, err)
	}
	return authContext(ctx, jwt, c)
}

func (a *RemoteAuthenticator) AuthenticateGRPCRequest(ctx context.Context) (interfaces.UserInfo, error) {
	ctx = a.AuthenticatedGRPCContext(ctx)
	if _, ok := ctx.Value(authutil.ContextTokenStringKey).(string); !ok {
		return nil, status.UnauthenticatedError("unauthenticated!")
	}
	return claims.ClaimsFromContext(ctx)
}

func (a *RemoteAuthenticator) FillUser(ctx context.Context, user *tables.User) error {
	return status.UnavailableError("User creation unsupported with remote authentication")
}

func (a *RemoteAuthenticator) AuthenticatedUser(ctx context.Context) (interfaces.UserInfo, error) {
	claims, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return claims, nil
}

func (a *RemoteAuthenticator) AuthContextFromAPIKey(ctx context.Context, apiKey string) context.Context {
	ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, apiKey)
	return a.AuthenticatedGRPCContext(ctx)
}

func (a *RemoteAuthenticator) TrustedJWTFromAuthContext(ctx context.Context) string {
	jwt, ok := ctx.Value(authutil.ContextTokenStringKey).(string)
	if !ok {
		return ""
	}
	return jwt
}

func (a *RemoteAuthenticator) AuthContextFromTrustedJWT(ctx context.Context, jwt string) context.Context {
	return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
}

func (a *RemoteAuthenticator) authenticate(ctx context.Context) (string, error) {
	sd := subdomain.Get(ctx)
	req := authpb.AuthenticateRequest{Subdomain: &sd}
	if useRSASignedJWTs(ctx, a.experimentFlagProvider) {
		req.JwtSigningMethod = authpb.JWTSigningMethod_RS256.Enum()
	}
	resp, err := a.authClient.Authenticate(ctx, &req)
	if err != nil {
		return "", err
	}
	if resp == nil {
		return "", status.InternalError("Authenticate succeeded with nil response")
	}
	if resp.Jwt == nil {
		return "", status.InternalError("Authenticate succeeded with nil jwt")
	}
	return *resp.Jwt, nil
}

// Returns:
// - A valid JWT from the incoming RPC metadata or
// - An error if an invalid JWT is present or
// - An empty string and no error if no JWT is present
func (a *RemoteAuthenticator) getValidJwtFromContext(ctx context.Context) (string, *claims.Claims, error) {
	jwt := getLastMetadataValue(ctx, authutil.ContextTokenStringKey)
	if jwt == "" {
		return "", nil, nil
	}
	claims, err := a.jwtIsValid(ctx, jwt)
	if err != nil {
		return "", nil, err
	}
	return jwt, claims, nil
}

func (a *RemoteAuthenticator) jwtIsValid(ctx context.Context, jwt string) (*claims.Claims, error) {
	// N.B. the ClaimsParser validates JWTs before returning them.
	claims, err := a.claimsParser.Parse(ctx, jwt)
	if err != nil {
		a.keyProvider.invalidateKeys()

		// Try again with fresh keys, in case keys were rotated or signing
		// method was changed.
		claims, err = a.claimsParser.Parse(ctx, jwt)
	}
	if err != nil {
		return nil, err
	}
	if claims.ExpiresAt < time.Now().Add(a.jwtExpirationBuffer).Unix() {
		return nil, status.NotFoundError("JWT will expire soon")
	}
	return claims, nil
}

func getLastMetadataValue(ctx context.Context, key string) string {
	values := metadata.ValueFromIncomingContext(ctx, key)
	if len(values) > 0 {
		return values[len(values)-1]
	}
	return ""
}

func authContext(ctx context.Context, jwt string, c *claims.Claims) context.Context {
	ctx = context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
	return claims.AuthContext(ctx, c)
}
