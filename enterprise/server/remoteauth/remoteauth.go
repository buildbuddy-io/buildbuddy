package remoteauth

import (
	"context"
	"crypto/rsa"
	"errors"
	"flag"
	"net/http"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/golang-jwt/jwt/v4"
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

	remoteAuthTarget              = flag.String("auth.remote_auth_target", "", "The gRPC target of the remote authentication API.")
	remoteAuthJwtExpirationBuffer = flag.Duration("auth.remote_auth_jwt_expiration_buffer", time.Minute, "Discard remote-auth minted JWTs if they're within this time buffer of their expiration time.")
	remoteAuthJWTSigningMethod    = flag.String("auth.remote_auth_jwt_signing_method", "HS256", "The signing method to use for remote authentication. (HS256 or RS256)")
)

func NewRemoteAuthenticator() (*RemoteAuthenticator, error) {
	conn, err := grpc_client.DialSimple(*remoteAuthTarget)
	if err != nil {
		return nil, err
	}
	return newRemoteAuthenticator(conn)
}

func newRemoteAuthenticator(conn grpc.ClientConnInterface) (*RemoteAuthenticator, error) {
	config := &lru.Config[string]{
		MaxSize: jwtCacheSize,
		SizeFn:  func(v string) int64 { return 1 },
	}
	cache, err := lru.NewLRU(config)
	if err != nil {
		return nil, err
	}
	claimsCache, err := claims.NewClaimsCache()
	if err != nil {
		return nil, err
	}
	pkConfig := &lru.Config[*rsa.PublicKey]{
		MaxSize: 100, // Small cache for parsed public keys
		SizeFn:  func(v *rsa.PublicKey) int64 { return 1 },
	}
	pkCache, err := lru.NewLRU(pkConfig)
	if err != nil {
		return nil, err
	}
	return &RemoteAuthenticator{
		authClient:          authpb.NewAuthServiceClient(conn),
		cache:               cache,
		jwtExpirationBuffer: *remoteAuthJwtExpirationBuffer,
		claimsCache:         claimsCache,
		publicKeyCache:      pkCache,
	}, nil
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
	authClient          authpb.AuthServiceClient
	cache               interfaces.LRU[string]
	jwtExpirationBuffer time.Duration
	mu                  sync.RWMutex // protects cache
	claimsCache         *claims.ClaimsCache

	pkMu               sync.Mutex // protects fetching and publicKeyCache
	lastPublicKeyFetch time.Time
	publicKeyCache     interfaces.LRU[*rsa.PublicKey]
	publicKeys         []string
}

// Admin stuff unsupported in remote authenticator.
func (a *RemoteAuthenticator) AdminGroupID() string {
	return ""
}

// TODO(iain): control via flag if needed.
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
	jwt, c, err := getValidJwtFromContext(ctx, a)
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
		if c, err := a.jwtIsValid(jwt); err == nil {
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
	c, err = a.jwtIsValid(jwt)
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
	req := &authpb.AuthenticateRequest{Subdomain: &sd}
	if *remoteAuthJWTSigningMethod == "RS256" {
		method := authpb.JWTSigningMethod_RS256
		req.JwtSigningMethod = &method
	}
	resp, err := a.authClient.Authenticate(ctx, req)
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

func (a *RemoteAuthenticator) fetchPublicKeys(ctx context.Context, force bool) ([]string, error) {
	a.pkMu.Lock()
	defer a.pkMu.Unlock()

	if !force && time.Since(a.lastPublicKeyFetch) < time.Minute && len(a.publicKeys) > 0 {
		return a.publicKeys, nil
	}

	resp, err := a.authClient.GetPublicKeys(ctx, &authpb.GetPublicKeysRequest{})
	if err != nil {
		return nil, err
	}

	var keys []string
	for _, pk := range resp.GetPublicKeys() {
		pem := pk.GetKey()
		keys = append(keys, pem)
		if _, ok := a.publicKeyCache.Get(pem); !ok {
			parsedKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(pem))
			if err != nil {
				log.Warningf("Failed to parse public key: %s", err)
				continue
			}
			a.publicKeyCache.Add(pem, parsedKey)
		}
	}
	a.publicKeys = keys
	a.lastPublicKeyFetch = time.Now()
	return keys, nil
}

// Returns:
// - A valid JWT from the incoming RPC metadata or
// - An error if an invalid JWT is present or
// - An empty string and no error if no JWT is present
func getValidJwtFromContext(ctx context.Context, a *RemoteAuthenticator) (string, *claims.Claims, error) {
	jwt := getLastMetadataValue(ctx, authutil.ContextTokenStringKey)
	if jwt == "" {
		return "", nil, nil
	}
	claims, err := a.jwtIsValid(jwt)
	if err != nil {
		return "", nil, err
	}
	return jwt, claims, nil
}

func (a *RemoteAuthenticator) jwtIsValid(token string) (*claims.Claims, error) {
	if *remoteAuthJWTSigningMethod == "RS256" {
		return a.jwtIsValidRS256(token, true /* retry */)
	}
	return a.jwtIsValidHS256(token)
}

func (a *RemoteAuthenticator) jwtIsValidHS256(token string) (*claims.Claims, error) {
	// N.B. the ClaimsCache validates JWTs before returning them.
	c, err := a.claimsCache.Get(token)
	if err != nil {
		return nil, err
	}
	if c.ExpiresAt < time.Now().Add(a.jwtExpirationBuffer).Unix() {
		return nil, status.NotFoundError("JWT will expire soon")
	}
	return c, nil
}

func (a *RemoteAuthenticator) jwtIsValidRS256(token string, retry bool) (*claims.Claims, error) {
	// Check parsed claims cache first.
	if c, err := a.claimsCache.GetWithKeys(token, a.getPublicKeys()...); err == nil {
		if c.ExpiresAt < time.Now().Add(a.jwtExpirationBuffer).Unix() {
			return nil, status.NotFoundError("JWT will expire soon")
		}
		return c, nil
	} else if !status.IsUnauthenticatedError(err) {
		// If it's a JWT validation error (not a BB status error), it might be a signature issue.
		var jwtErr *jwt.ValidationError
		if !errors.As(err, &jwtErr) {
			return nil, err
		}
	}

	// If failed, try to refetch keys if retry is true.
	if retry {
		if _, err := a.fetchPublicKeys(context.TODO(), true /* force */); err != nil {
			log.Warningf("Failed to fetch public keys: %s", err)
		}
		return a.jwtIsValidRS256(token, false)
	}

	return nil, status.UnauthenticatedError("invalid JWT signature")
}

func (a *RemoteAuthenticator) getPublicKeys() []interface{} {
	a.pkMu.Lock()
	defer a.pkMu.Unlock()
	var keys []interface{}
	for _, k := range a.publicKeys {
		if pk, ok := a.publicKeyCache.Get(k); ok {
			keys = append(keys, pk)
		} else {
			keys = append(keys, k)
		}
	}
	return keys
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
