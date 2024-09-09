package auth_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

var (
	jwtRsaPublicKey    = flag.String("auth.jwt_rsa_public_key", "set_the_jwt_in_config", "The RSA256 public key to use for verifying JWTs returned by the AuthService.")
	newJwtRsaPublicKey = flag.String("auth.new_jwt_rsa_public_key", "", "The *NEW* RSA256 public key to use for verifying JWTs returned by the AuthService. This exists to support key rotation.")
)

type AuthService struct {
	authenticator interfaces.GRPCAuthenticator
	publicKeys    []*authpb.PublicKey
}

func Register(env *real_environment.RealEnv) {
	keys := []*authpb.PublicKey{&authpb.PublicKey{Key: jwtRsaPublicKey}}
	if *newJwtRsaPublicKey != "" {
		keys = []*authpb.PublicKey{
			&authpb.PublicKey{Key: newJwtRsaPublicKey},
			&authpb.PublicKey{Key: jwtRsaPublicKey},
		}
	}
	env.SetAuthService(AuthService{
		authenticator: env.GetAuthenticator(),
		publicKeys:    keys,
	})
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
	return &authpb.GetPublicKeysResponse{PublicKeys: a.publicKeys}, nil
}
