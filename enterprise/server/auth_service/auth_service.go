package auth_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

type AuthService struct {
	authenticator interfaces.GRPCAuthenticator
}

func Register(env *real_environment.RealEnv) {
	env.SetAuthService(AuthService{authenticator: env.GetAuthenticator()})
}

func (a AuthService) Authenticate(ctx context.Context, req *authpb.AuthenticateRequest) (*authpb.AuthenticateResponse, error) {
	userInfo, err := a.authenticator.AuthenticateGRPCRequest(ctx)
	if err != nil {
		return nil, err
	}
	jwt, err := userInfo.AssembleJWT()
	if err != nil {
		return nil, err
	}
	return &authpb.AuthenticateResponse{Jwt: &jwt}, nil
}
