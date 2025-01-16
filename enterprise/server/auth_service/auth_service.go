package auth_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

type AuthService struct {
	authenticator interfaces.GRPCAuthenticator
}

func Register(env *real_environment.RealEnv) {
	env.SetAuthService(AuthService{authenticator: env.GetAuthenticator()})
}

func (a AuthService) Authenticate(ctx context.Context, req *authpb.AuthenticateRequest) (*authpb.AuthenticateResponse, error) {
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
	return &authpb.GetPublicKeysResponse{}, status.UnimplementedError("GetPublicKeys unimplemented")
}
