package auth_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

type AuthService struct {
	authenticator interfaces.Authenticator
}

func Register(env *real_environment.RealEnv) {
	env.SetAuthService(AuthService{authenticator: env.GetAuthenticator()})
}

func (a AuthService) Authenticate(ctx context.Context, req *authpb.AuthenticateRequest) (*authpb.AuthenticateResponse, error) {
	// TODO(iain): is this OK / check TTL.
	jwt := a.authenticator.TrustedJWTFromAuthContext(ctx)
	return &authpb.AuthenticateResponse{Jwt: &jwt}, nil
}
