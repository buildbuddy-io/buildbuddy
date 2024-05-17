package auth_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

type AuthService struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) error {
	auth := AuthService{}
	env.SetAuthService(auth)
	return nil
}

func (a AuthService) Authenticate(ctx context.Context, req *authpb.AuthenticateRequest) (*authpb.AuthenticateResponse, error) {
	var resp authpb.AuthenticateResponse
	return &resp, nil
}
