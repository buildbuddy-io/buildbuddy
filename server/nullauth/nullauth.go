package nullauth

import (
	"context"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// Create these once so they don't show up in benchmarks that use null auth
var (
	anonymouseUserError = authutil.AnonymousUserError("Auth not implemented")
	unimplementedError  = status.UnimplementedError("Auth not implemented")
)

func NewNullAuthenticator(anonymousUsageEnabled bool, adminGroupID string) *NullAuthenticator {
	return &NullAuthenticator{
		adminGroupID:           adminGroupID,
		anonymousUsageDisabled: !anonymousUsageEnabled,
	}
}

type NullAuthenticator struct {
	adminGroupID           string
	anonymousUsageDisabled bool
}

func (a *NullAuthenticator) AdminGroupID() string {
	return a.adminGroupID
}

func (a *NullAuthenticator) AnonymousUsageEnabled(ctx context.Context) bool {
	return !a.anonymousUsageDisabled
}

func (a *NullAuthenticator) PublicIssuers() []string {
	return []string{}
}

func (a *NullAuthenticator) SSOEnabled() bool {
	return false
}

func (a *NullAuthenticator) AuthenticatedHTTPContext(w http.ResponseWriter, r *http.Request) context.Context {
	return r.Context()
}

func (a *NullAuthenticator) AuthenticatedGRPCContext(ctx context.Context) context.Context {
	return ctx
}

func (a *NullAuthenticator) AuthenticatedUser(ctx context.Context) (interfaces.UserInfo, error) {
	return nil, anonymouseUserError
}

func (a *NullAuthenticator) FillUser(ctx context.Context, user *tables.User) error {
	return nil
}

func (a *NullAuthenticator) Login(w http.ResponseWriter, r *http.Request) error {
	return unimplementedError
}

func (a *NullAuthenticator) Auth(w http.ResponseWriter, r *http.Request) error {
	return unimplementedError
}

func (a *NullAuthenticator) Logout(w http.ResponseWriter, r *http.Request) error {
	return unimplementedError
}

func (a *NullAuthenticator) AuthContextFromAPIKey(ctx context.Context, apiKey string) context.Context {
	return ctx
}

func (a *NullAuthenticator) AuthenticateGRPCRequest(ctx context.Context) (interfaces.UserInfo, error) {
	return nil, nil
}

func (a *NullAuthenticator) TrustedJWTFromAuthContext(ctx context.Context) string {
	return ""
}

func (a *NullAuthenticator) AuthContextFromTrustedJWT(ctx context.Context, jwt string) context.Context {
	return ctx
}
