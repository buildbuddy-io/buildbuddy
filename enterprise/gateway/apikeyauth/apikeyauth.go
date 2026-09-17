// Package apikeyauth authenticates gateway callers by their BuildBuddy API
// key, resolved by the app.
package apikeyauth

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/gatewayauth"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

type authenticator struct {
	auth interfaces.Authenticator
}

func New(a interfaces.Authenticator) gatewayauth.Authenticator {
	return &authenticator{auth: a}
}

func (a *authenticator) Authenticate(ctx context.Context, _ string) (*gatewayauth.Principal, error) {
	claims, err := a.auth.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	return &gatewayauth.Principal{
		User:      claims.GetUserID(),
		Namespace: claims.GetGroupID(),
	}, nil
}
