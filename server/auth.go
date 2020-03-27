package auth

import (
	"context"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

type NullAuthenticator struct{}

func (a *NullAuthenticator) AuthenticateRequest(w http.ResponseWriter, r *http.Request) (context.Context, error) {
	return context.WithValue(r.Context(), "null-auth", "1"), nil
}
func (a *NullAuthenticator) GetUser(ctx context.Context) interfaces.UserInfo {
	return nil
}
