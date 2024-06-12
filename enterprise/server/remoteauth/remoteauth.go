package remoteauth

import (
	"context"
	"flag"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

const (
	jwtCacheSize = 10_000
)

var (
	authHeaders = []string{authutil.APIKeyHeader}

	remoteAuthTarget = flag.String("auth.remote_auth_target", "grpcs://remote.buildbuddy.dev", "The gRPC target of the remote authentication API.")
)

func NewRemoteAuthenticator() (*RemoteAuthenticator, error) {
	conn, err := grpc_client.DialSimpleWithoutPooling(*remoteAuthTarget)
	if err != nil {
		return nil, err
	}
	return newRemoteAuthenticator(conn)
}

func newRemoteAuthenticator(conn *grpc.ClientConn) (*RemoteAuthenticator, error) {
	config := &lru.Config[string]{
		MaxSize: jwtCacheSize,
		SizeFn:  func(v string) int64 { return 1 },
	}
	cache, err := lru.NewLRU(config)
	if err != nil {
		return nil, err
	}
	return &RemoteAuthenticator{
		authClient: authpb.NewAuthServiceClient(conn),
		cache:      cache,
	}, nil
}

type RemoteAuthenticator struct {
	authClient authpb.AuthServiceClient
	cache      interfaces.LRU[string]
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
	key, err := hashAuthHeaders(ctx)
	if err != nil {
		jwt, err := a.authenticate(ctx)
		if err != nil {
			return ctx
		}
		return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
	}

	jwt, found := a.cache.Get(key)
	if found {
		return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
	}
	jwt, err = a.authenticate(ctx)
	if err != nil {
		return ctx
	}
	a.cache.Add(key, jwt)
	return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
}

func (a *RemoteAuthenticator) AuthenticateGRPCRequest(ctx context.Context) (interfaces.UserInfo, error) {
	ctx = a.AuthenticatedGRPCContext(ctx)
	_, ok := ctx.Value(authutil.ContextTokenStringKey).(string)
	if !ok {
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

func hashAuthHeaders(ctx context.Context) (string, error) {
	input := []string{}
	for _, key := range authHeaders {
		values := metadata.ValueFromIncomingContext(ctx, key)
		if len(values) > 0 {
			input = append(input, key)
			input = append(input, values...)
		}
	}
	if len(input) == 0 {
		return "", status.InvalidArgumentError("no auth headers")
	}
	return hash.Strings(input...), nil
}
