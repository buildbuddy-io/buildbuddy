package remoteauth

import (
	"context"
	"flag"
	"net/http"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
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

	remoteAuthTarget = flag.String("auth.remote_auth_target", "", "The gRPC target of the remote authentication API.")
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
	return &RemoteAuthenticator{
		authClient:  authpb.NewAuthServiceClient(conn),
		cache:       cache,
		claimsCache: claimsCache,
	}, nil
}

type RemoteAuthenticator struct {
	authClient  authpb.AuthServiceClient
	cache       interfaces.LRU[string]
	mu          sync.RWMutex // protects cache
	claimsCache *claims.ClaimsCache
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
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()

	// If a JWT was provided, check if it's valid and use it if so.
	jwt, err := validateJWT(ctx, a.claimsCache)
	if err != nil {
		return authutil.AuthContextWithError(ctx, err)
	}
	if jwt != "" {
		return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
	}

	key := getAPIKey(ctx)
	if key == "" {
		return authutil.AuthContextWithError(ctx, status.PermissionDeniedError("Missing API key"))
	}
	a.mu.RLock()
	jwt, found := a.cache.Get(key)
	a.mu.RUnlock()
	if found {
		return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
	}
	jwt, err = a.authenticate(ctx)
	if err != nil {
		log.Debugf("Error remotely authenticating: %s", err)
		return authutil.AuthContextWithError(ctx, err)
	}
	a.mu.Lock()
	a.cache.Add(key, jwt)
	a.mu.Unlock()
	return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
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
	resp, err := a.authClient.Authenticate(ctx, &authpb.AuthenticateRequest{})
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

func getAPIKey(ctx context.Context) string {
	return getLastMetadataValue(ctx, authutil.APIKeyHeader)
}

// Returns a valid JWT from the incoming RPC metadata, or an error an invalid
// JWT is present, or an empty string and no error if no JWT is provided.
func validateJWT(ctx context.Context, claimsCache *claims.ClaimsCache) (string, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()

	jwt := getLastMetadataValue(ctx, authutil.ContextTokenStringKey)
	if jwt == "" {
		return "", nil
	}
	_, err := claimsCache.Get(jwt)
	if err != nil {
		return "", err
	}
	return jwt, nil
}

func getLastMetadataValue(ctx context.Context, key string) string {
	values := metadata.ValueFromIncomingContext(ctx, key)
	if len(values) > 0 {
		return values[len(values)-1]
	}
	return ""
}
