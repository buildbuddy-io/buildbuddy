package nullauth

import (
	"context"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type NullAuthenticator struct{}

func (a *NullAuthenticator) AuthenticatedHTTPContext(w http.ResponseWriter, r *http.Request) context.Context {
	return r.Context()
}

func (a *NullAuthenticator) AuthenticatedGRPCContext(ctx context.Context) context.Context {
	return ctx
}

func (a *NullAuthenticator) AuthenticatedUser(ctx context.Context) (interfaces.UserInfo, error) {
	return nil, status.UnauthenticatedError("Auth not implemented")
}

func (a *NullAuthenticator) FillUser(ctx context.Context, user *tables.User) error {
	return nil
}

func (a *NullAuthenticator) Login(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func (a *NullAuthenticator) Auth(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func (a *NullAuthenticator) Logout(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func (a *NullAuthenticator) ParseAPIKeyFromString(input string) string {
	return ""
}

func (a *NullAuthenticator) AuthContextFromAPIKey(ctx context.Context, apiKey string) context.Context {
	return ctx
}

func (a *NullAuthenticator) AuthenticateGRPCRequest(ctx context.Context) (interfaces.UserInfo, error) {
	return nil, nil
}
