package ociauth

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokenAuthenticator(t *testing.T) {
	auth := NewTokenAuthenticator[string](TokenAuthenticatorOpts{
		TokenTTL:   time.Minute,
		MaxEntries: 2,
	})

	require.False(t, auth.IsAuthorized("one"))

	auth.Refresh("one")
	require.True(t, auth.IsAuthorized("one"))

	auth.Refresh("two")
	require.True(t, auth.IsAuthorized("one"))

	auth.Refresh("three")
	require.True(t, auth.IsAuthorized("one"), "recently accessed token should be retained")
	require.False(t, auth.IsAuthorized("two"), "least-recently used token should be evicted")
	require.True(t, auth.IsAuthorized("three"))
}

func TestTokenAuthenticatorExpiry(t *testing.T) {
	auth := NewTokenAuthenticator[string](TokenAuthenticatorOpts{
		TokenTTL: -time.Second,
	})

	auth.Refresh("expired")
	require.False(t, auth.IsAuthorized("expired"))
}

func TestTokenAuthenticatorAuthorize(t *testing.T) {
	auth := NewTokenAuthenticator[string](TokenAuthenticatorOpts{
		TokenTTL: time.Minute,
	})

	proveCount := 0
	prove := func(context.Context) error {
		proveCount++
		return nil
	}

	require.NoError(t, auth.Authorize(context.Background(), "token", prove))
	require.NoError(t, auth.Authorize(context.Background(), "token", prove))
	require.Equal(t, 1, proveCount)
}
