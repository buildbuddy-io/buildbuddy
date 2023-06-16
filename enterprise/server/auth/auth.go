package auth

import (
	"context"
	"flag"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oidc"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/saml"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/golang-jwt/jwt"
)

var (
	adminGroupID = flag.String("auth.admin_group_id", "", "ID of a group whose members can perform actions only accessible to server admins.")
)

func Register(ctx context.Context, env environment.Env) error {
	httpAuthenticators := []interfaces.HTTPAuthenticator{}
	userAuthenticators := []interfaces.UserAuthenticator{}

	oidc, err := oidc.NewOpenIDAuthenticator(ctx, env, *adminGroupID)
	if err != nil {
		return status.InternalErrorf("OIDC authenticator failed to configure: %v", err)
	}
	httpAuthenticators = append(httpAuthenticators, oidc)
	userAuthenticators = append(userAuthenticators, oidc)

	if saml.IsEnabled(env) {
		samlAuthenticator := saml.NewSAMLAuthenticator(env)
		httpAuthenticators = append(httpAuthenticators, samlAuthenticator)
		userAuthenticators = append(userAuthenticators, samlAuthenticator)
	}

	authenticator := &authenticator{
		http: httpAuthenticators,
		user: userAuthenticators,
		// TODO(siggisim): For the authenticators below, either:
		// - Break them out of the OIDCAuthenticator into their own packages
		// - Pull the implementation into the authenticator in this file
		// - Delete them altogether
		// - Some combination of the above (likely delete installation and break out or pull in the other two)
		GRPCAuthenticator:         oidc,
		APIKeyAuthenticator:       oidc,
		InstallationAuthenticator: oidc,
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

type authenticator struct {
	interfaces.GRPCAuthenticator
	interfaces.APIKeyAuthenticator
	interfaces.InstallationAuthenticator

	http []interfaces.HTTPAuthenticator
	user []interfaces.UserAuthenticator
}

func (a *authenticator) Login(w http.ResponseWriter, r *http.Request) error {
	var err error
	for _, authenticator := range a.http {
		if err = authenticator.Login(w, r); err == nil {
			return nil
		}
	}
	if err != nil {
		return err
	}
	return status.NotFoundErrorf("No authenticator registered to handle login path: %s", r.URL.Path)
}
func (a *authenticator) Logout(w http.ResponseWriter, r *http.Request) error {
	for _, authenticator := range a.http {
		authenticator.Logout(w, r)
	}
	return status.UnauthenticatedError("Logged out!")
}
func (a *authenticator) Auth(w http.ResponseWriter, r *http.Request) error {
	var err error
	for _, authenticator := range a.http {
		if err = authenticator.Auth(w, r); err == nil {
			return nil
		}
	}
	if err != nil {
		return err
	}
	return status.NotFoundErrorf("No authenticator registered to auth path: %s", r.URL.Path)
}

func (a *authenticator) AuthenticatedHTTPContext(w http.ResponseWriter, r *http.Request) context.Context {
	ctx := r.Context()
	for _, authenticator := range a.http {
		ctx = authenticator.AuthenticatedHTTPContext(w, r)
		r = r.WithContext(ctx)
	}
	return ctx
}

func (a *authenticator) SSOEnabled() bool {
	for _, authenticator := range a.http {
		if authenticator.SSOEnabled() {
			return true
		}
	}
	return false
}

func (a *authenticator) FillUser(ctx context.Context, user *tables.User) error {
	errors := make([]error, 0)
	for _, auth := range a.user {
		if err := auth.FillUser(ctx, user); err != nil {
			errors = append(errors, err)
		} else {
			return nil
		}
	}
	if len(errors) > 0 {
		return errors[0]
	}
	return status.UnauthenticatedErrorf("No user authenticators configured")
}

func (a *authenticator) AuthenticatedUser(ctx context.Context) (interfaces.UserInfo, error) {
	errors := make([]error, 0)
	for _, auth := range a.user {
		if user, err := auth.AuthenticatedUser(ctx); err != nil {
			errors = append(errors, err)
		} else {
			return user, err
		}
	}
	if len(errors) > 0 {
		return nil, errors[0]
	}
	return nil, status.UnauthenticatedErrorf("No user authenticators configured")
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
