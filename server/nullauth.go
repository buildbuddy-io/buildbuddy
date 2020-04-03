package nullauth

import (
	"context"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type NullAuthenticator struct{}

func (a *NullAuthenticator) AuthenticateRequest(w http.ResponseWriter, r *http.Request) (context.Context, error) {
	return r.Context(), nil
}

func (a *NullAuthenticator) GetUserToken(ctx context.Context) (interfaces.UserToken, error) {
	return nil, status.UnimplementedError("Not implemented")
}

func (a *NullAuthenticator) FillUser(ctx context.Context, user *tables.User) error {
	return nil
}

func (a *NullAuthenticator) Login(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/", 301)
}

func (a *NullAuthenticator) Auth(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/", 301)
}

func (a *NullAuthenticator) Logout(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/", 301)
}
