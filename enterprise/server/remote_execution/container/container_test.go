package container_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func userCtx(t *testing.T, ta *testauth.TestAuthenticator, userID string) context.Context {
	ctx, err := ta.WithAuthenticatedUser(context.Background(), userID)
	require.NoError(t, err)
	return ctx
}

func TestImageCacheAuthenticator(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1", "US2", "GR2"))
	env.SetAuthenticator(ta)

	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})

	{
		token1, err := container.NewImageCacheToken(
			userCtx(t, ta, "US1"),
			env,
			&container.PullCredentials{Username: "user1", Password: "pass1"},
			"gcr.io/org1/image:latest",
		)
		t.Log("token1: " + token1)

		require.NoError(t, err)

		isAuthorized := auth.IsAuthorized(token1)

		assert.False(t, isAuthorized, "Tokens should not be initially authorized")
	}

	{
		token1, err := container.NewImageCacheToken(
			userCtx(t, ta, "US1"),
			env,
			&container.PullCredentials{Username: "user1", Password: "pass1"},
			"gcr.io/org1/image:latest",
		)
		t.Log("token1: " + token1)

		require.NoError(t, err)

		auth.Refresh(token1)
		isAuthorized := auth.IsAuthorized(token1)

		assert.True(t, isAuthorized, "Tokens should be authorized after authenticating with the remote registry")
	}

	{
		token2, err := container.NewImageCacheToken(
			userCtx(t, ta, "US2"),
			env,
			&container.PullCredentials{Username: "user1", Password: "pass1"},
			"gcr.io/org1/image:latest",
		)
		t.Log("token2: " + token2)

		require.NoError(t, err)

		isAuthorized := auth.IsAuthorized(token2)

		assert.False(
			t, isAuthorized,
			"Groups should not be able to access cached images until they re-authenticate with the registry, "+
				"even if using the same creds used previously by another group")
	}

	{
		anonToken, err := container.NewImageCacheToken(
			context.Background(),
			env,
			/* creds=*/ nil,
			"alpine:latest",
		)
		t.Log("anonToken: " + anonToken)

		require.NoError(t, err)

		isAuthorized := auth.IsAuthorized(anonToken)

		assert.False(
			t, isAuthorized,
			"Publicly accessible images still need an initial auth check, "+
				"since we don't know ahead of time whether images are public",
		)
	}

	{
		anonToken, err := container.NewImageCacheToken(
			context.Background(),
			env,
			/* creds=*/ nil,
			"alpine:latest",
		)
		t.Log("anonToken: " + anonToken)

		require.NoError(t, err)

		auth.Refresh(anonToken)
		isAuthorized := auth.IsAuthorized(anonToken)

		assert.True(t, isAuthorized, "Anonymous access should work after refreshing")
	}

	{
		opts := container.ImageCacheAuthenticatorOpts{
			// Have tokens expire in the past relative to when they are created, so we
			// can test expiration
			TokenTTL: -1 * time.Second,
		}
		auth := container.NewImageCacheAuthenticator(opts)

		immediatelyExpiredToken, err := container.NewImageCacheToken(
			userCtx(t, ta, "US1"),
			env,
			&container.PullCredentials{Username: "user1", Password: "pass1"},
			"gcr.io/org1/image:latest",
		)
		t.Log("immediatelyExpiredToken: " + immediatelyExpiredToken)

		require.NoError(t, err)

		auth.Refresh(immediatelyExpiredToken)
		isAuthorized := auth.IsAuthorized(immediatelyExpiredToken)

		assert.False(t, isAuthorized, "Expired tokens should not be authorized")
	}
}
