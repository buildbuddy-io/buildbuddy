package nullauth

import (
	"context"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

type NullAuthenticator struct{}

type emptyUserToken struct{}

func (e *emptyUserToken) GetIssuer() string {
	return ""
}
func (e *emptyUserToken) GetSubscriberID() string {
	return ""
}

func (a *NullAuthenticator) AuthenticateRequest(w http.ResponseWriter, r *http.Request) (context.Context, error) {
	return context.WithValue(r.Context(), "null-auth", "1"), nil
}

func (a *NullAuthenticator) GetUserToken(ctx context.Context) interfaces.UserToken {
	return &emptyUserToken{}
}

func (a *NullAuthenticator) GetUser(ctx context.Context) interfaces.User {
	return nil
}
