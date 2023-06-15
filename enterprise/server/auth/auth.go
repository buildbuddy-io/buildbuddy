package auth

import (
	"context"
	"flag"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oidc"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/golang-jwt/jwt"
)

var (
	adminGroupID = flag.String("auth.admin_group_id", "", "ID of a group whose members can perform actions only accessible to server admins.")
)

func Register(ctx context.Context, env environment.Env) error {
	authenticator, err := oidc.NewOpenIDAuthenticator(ctx, env, *adminGroupID)
	if err != nil {
		return status.InternalErrorf("Authenticator failed to configure: %v", err)
	}
	env.SetAuthenticator(authenticator)
	return nil
}

func RegisterNullAuth(env environment.Env) error {
	env.SetAuthenticator(
		nullauth.NewNullAuthenticator(
			oidc.AnonymousUsageEnabled(),
			*adminGroupID,
		),
	)
	return nil
}

// Parses the JWT's UserInfo from the context without verifying the JWT.
// Only use this if you know what you're doing and the JWT is coming from a trusted source
// that has already verified its authenticity.
func UserFromTrustedJWT(ctx context.Context) (interfaces.UserInfo, error) {
	if tokenString, ok := ctx.Value(authutil.ContextTokenStringKey).(string); ok && tokenString != "" {
		claims := &claims.Claims{}
		parser := jwt.Parser{}
		_, _, err := parser.ParseUnverified(tokenString, claims)
		if err != nil {
			return nil, err
		}
		return claims, nil
	}
	// WARNING: app/auth/auth_service.ts depends on this status being UNAUTHENTICATED.
	return nil, authutil.AnonymousUserError(authutil.UserNotFoundMsg)
}
