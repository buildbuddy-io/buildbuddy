package auth_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

type AuthService struct {
}

func Register(env *real_environment.RealEnv) {
	env.SetAuthService(AuthService{})
}

func (a AuthService) Authenticate(ctx context.Context, req *authpb.AuthenticateRequest) (*authpb.AuthenticateResponse, error) {
	return &authpb.AuthenticateResponse{}, nil
}
