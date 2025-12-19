package auth_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/golang-jwt/jwt/v4"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

type AuthService struct {
	authenticator interfaces.GRPCAuthenticator
}

func Register(env *real_environment.RealEnv) error {
	if err := claims.Init(); err != nil {
		return err
	}
	env.SetAuthService(AuthService{authenticator: env.GetAuthenticator()})
	return nil
}

func (a AuthService) Authenticate(ctx context.Context, req *authpb.AuthenticateRequest) (*authpb.AuthenticateResponse, error) {
	// Override the subdomain with the one for which auth is requested
	ctx = subdomain.Context(ctx, req.GetSubdomain())
	ctx = a.authenticator.AuthenticatedGRPCContext(ctx)
	err, found := authutil.AuthErrorFromContext(ctx)
	if found {
		return nil, err
	}

	// TODO(iain): this is inefficient because it's minting JWTs twice (an
	// HMAC-SHA256-signed one in AuthenticatedGRPCContext() above, and an
	// RSA-256-signed one here). Fix this by cleaning up the authentication
	// logic a bit and exposing the right way to get just the JWT we need.
	if req.GetJwtSigningMethod() == authpb.JWTSigningMethod_RS256 {
		userInfo, err := claims.ClaimsFromContext(ctx)
		if err != nil {
			return nil, err
		}
		jwt, err := claims.AssembleJWT(userInfo, jwt.SigningMethodRS256)
		if err != nil {
			return nil, err
		}
		return &authpb.AuthenticateResponse{Jwt: &jwt}, nil
	} else {
		jwt, ok := ctx.Value(authutil.ContextTokenStringKey).(string)
		if ok {
			return &authpb.AuthenticateResponse{Jwt: &jwt}, nil
		}
		return nil, status.UnauthenticatedError("Authentication failed")
	}
}

func (a AuthService) GetPublicKeys(ctx context.Context, req *authpb.GetPublicKeysRequest) (*authpb.GetPublicKeysResponse, error) {
	keys := claims.GetJWTKeys(authpb.JWTSigningMethod_RS256)
	publicKeys := make([]*authpb.PublicKey, 0, len(keys))
	for _, key := range keys {
		s, ok := key.(string)
		if !ok {
			continue
		}
		publicKeys = append(publicKeys, &authpb.PublicKey{Key: &s})
	}
	return &authpb.GetPublicKeysResponse{PublicKeys: publicKeys}, nil
}
