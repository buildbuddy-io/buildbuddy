package claims_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func contextWithUnverifiedJWT(c *claims.Claims) context.Context {
	authCtx := claims.AuthContextFromClaims(context.Background(), c, nil)
	jwt := authCtx.Value(authutil.ContextTokenStringKey).(string)
	return context.WithValue(context.Background(), authutil.ContextTokenStringKey, jwt)
}

func TestJWT(t *testing.T) {
	c := &claims.Claims{UserID: "US123"}
	testContext := contextWithUnverifiedJWT(c)

	parsedClaims, err := claims.ClaimsFromContext(testContext)
	require.NoError(t, err)
	require.Equal(t, c, parsedClaims)
}

func TestInvalidJWTKey(t *testing.T) {
	c := &claims.Claims{UserID: "US123"}
	testContext := contextWithUnverifiedJWT(c)

	// Validation should fail since the JWT above was signed using a different
	// key.
	flags.Set(t, "auth.jwt_key", "foo")
	_, err := claims.ClaimsFromContext(testContext)
	require.ErrorContains(t, err, "signature is invalid")
}

func TestJWTKeyRotation(t *testing.T) {
	c := &claims.Claims{UserID: "US123"}

	// Get JWT signed using old key.
	testContext := contextWithUnverifiedJWT(c)

	// Validate with both keys in place.
	flags.Set(t, "auth.new_jwt_key", "new_jwt_key")
	parsedClaims, err := claims.ClaimsFromContext(testContext)
	require.NoError(t, err)
	require.Equal(t, c, parsedClaims)

	// Get JWT signed using new key.
	testContext = contextWithUnverifiedJWT(c)
	// Validate with both keys in place.
	flags.Set(t, "auth.new_jwt_key", "new_jwt_key")
	parsedClaims, err = claims.ClaimsFromContext(testContext)
	require.NoError(t, err)
	require.Equal(t, c, parsedClaims)
}
