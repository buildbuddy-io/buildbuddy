package auth_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"
)

func makeJWT(t *testing.T, groupID string) string {
	// UserFromTrustedJWT doesn't verify signatures, so any key works here.
	tokenString, err := jwt.NewWithClaims(jwt.SigningMethodHS256, &claims.Claims{GroupID: groupID}).SignedString([]byte("test-key"))
	require.NoError(t, err)
	return tokenString
}

func TestUserFromTrustedJWT_ReusesClaimsFromContextWithTrustedJWT(t *testing.T) {
	// Put a JWT for GR1 in the context, the way the executor does when it
	// starts a task.
	ctx := auth.ContextWithTrustedJWT(t.Context(), makeJWT(t, "GR1"))

	// Looking up the user twice should return the claims parsed when the JWT
	// was added to the context, rather than parsing the JWT on each call.
	first, err := auth.UserFromTrustedJWT(ctx)
	require.NoError(t, err)
	second, err := auth.UserFromTrustedJWT(ctx)
	require.NoError(t, err)
	groupID := first.GetGroupID()
	require.Equal(t, "GR1", groupID)
	require.Same(t, first, second)
}

func TestUserFromTrustedJWT_ReplacedJWT(t *testing.T) {
	// Put a JWT for GR1 in the context with its claims cached, then replace
	// the JWT with one for GR2 without going through ContextWithTrustedJWT.
	ctx := auth.ContextWithTrustedJWT(t.Context(), makeJWT(t, "GR1"))
	ctx = context.WithValue(ctx, authutil.ContextTokenStringKey, makeJWT(t, "GR2"))

	// The cached claims no longer match the JWT in the context, so the user
	// should come from the replacement JWT.
	user, err := auth.UserFromTrustedJWT(ctx)
	require.NoError(t, err)
	groupID := user.GetGroupID()
	require.Equal(t, "GR2", groupID)
}

func TestUserFromTrustedJWT_EmptyJWT(t *testing.T) {
	// Tasks without a JWT get an empty one in the context.
	ctx := auth.ContextWithTrustedJWT(t.Context(), "")

	// That should still be treated as an anonymous user.
	_, err := auth.UserFromTrustedJWT(ctx)
	isAnonymous := authutil.IsAnonymousUserError(err)
	require.True(t, isAnonymous, "expected anonymous user error, got %v", err)
}
