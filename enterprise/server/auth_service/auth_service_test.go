package auth_service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

func TestAuthenticate(t *testing.T) {
	service := AuthService{}
	_, err := service.Authenticate(context.Background(), &authpb.AuthenticateRequest{})
	assert.NoError(t, err)
}
