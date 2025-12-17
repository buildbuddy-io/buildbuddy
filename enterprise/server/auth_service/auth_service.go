package auth_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

type AuthService struct {
	authenticator interfaces.GRPCAuthenticator
}

func Register(env *real_environment.RealEnv) {
	env.SetAuthService(AuthService{authenticator: env.GetAuthenticator()})
}

func (a AuthService) Authenticate(ctx context.Context, req *authpb.AuthenticateRequest) (*authpb.AuthenticateResponse, error) {
	// Override the subdomain with the one for which auth is requested
	ctx = subdomain.Context(ctx, req.GetSubdomain())
	ctx = a.authenticator.AuthenticatedGRPCContext(ctx)
	err, found := authutil.AuthErrorFromContext(ctx)
	if found {
		return nil, err
	}
	jwt, ok := ctx.Value(authutil.ContextTokenStringKey).(string)
	if ok {
		return &authpb.AuthenticateResponse{Jwt: &jwt}, nil
	}
	return nil, status.UnauthenticatedError("Authentication failed")
}

func (a AuthService) GetPublicKeys(ctx context.Context, req *authpb.GetPublicKeysRequest) (*authpb.GetPublicKeysResponse, error) {
	keys := claims.GetRSAPublicKeys()
	publicKeys := make([]*authpb.PublicKey, len(keys))
	for i, key := range claims.GetRSAPublicKeys() {
		publicKeys[i] = &authpb.PublicKey{Key: &key}
	}
	return &authpb.GetPublicKeysResponse{PublicKeys: publicKeys}, nil
}
