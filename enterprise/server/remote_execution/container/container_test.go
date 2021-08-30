package container_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/config"
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
			container.PullCredentials{Username: "user1", Password: "pass1"},
			"gcr.io/org1/image:latest",
		)

		require.NoError(t, err)

		isAuthorized := auth.IsAuthorized(token1)

		assert.False(t, isAuthorized, "Tokens should not be initially authorized")
	}

	{
		token1, err := container.NewImageCacheToken(
			userCtx(t, ta, "US1"),
			env,
			container.PullCredentials{Username: "user1", Password: "pass1"},
			"gcr.io/org1/image:latest",
		)

		require.NoError(t, err)

		auth.Refresh(token1)
		isAuthorized := auth.IsAuthorized(token1)

		assert.True(t, isAuthorized, "Tokens should be authorized after authenticating with the remote registry")
	}

	{
		token2, err := container.NewImageCacheToken(
			userCtx(t, ta, "US2"),
			env,
			container.PullCredentials{Username: "user1", Password: "pass1"},
			"gcr.io/org1/image:latest",
		)

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
			container.PullCredentials{},
			"alpine:latest",
		)

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
			container.PullCredentials{},
			"alpine:latest",
		)

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
			container.PullCredentials{Username: "user1", Password: "pass1"},
			"gcr.io/org1/image:latest",
		)

		require.NoError(t, err)

		auth.Refresh(immediatelyExpiredToken)
		isAuthorized := auth.IsAuthorized(immediatelyExpiredToken)

		assert.False(t, isAuthorized, "Expired tokens should not be authorized")
	}
}

func TestGetPullCredentials(t *testing.T) {
	env := testenv.GetTestEnv(t)

	env.GetConfigurator().GetExecutorConfig().ContainerRegistries = []config.ContainerRegistryConfig{
		{
			Hostnames: []string{"gcr.io", "us.gcr.io", "eu.gcr.io", "asia.gcr.io", "marketplace.gcr.io"},
			Username:  "gcruser",
			Password:  "gcrpass",
		},
		{
			Hostnames: []string{"docker.io"},
			Username:  "dockeruser",
			Password:  "dockerpass",
		},
	}

	for _, testCase := range []struct {
		imageRef            string
		expectedCredentials container.PullCredentials
	}{
		// Creds shouldn't be returned if there's no container image requested
		{"", container.PullCredentials{}},
		// Creds shouldn't be returned if the registry is unrecognized
		{"unrecognized-registry.io/foo/bar", container.PullCredentials{}},
		{"unrecognized-registry.io/foo/bar:latest", container.PullCredentials{}},
		{"unrecognized-registry.io/foo/bar@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", container.PullCredentials{}},
		// Images with no domain should get defaulted to docker.io (Docker and
		// other tools like `skopeo` assume docker.io as the default registry)
		{"alpine", creds("dockeruser", "dockerpass")},
		{"alpine:latest", creds("dockeruser", "dockerpass")},
		{"alpine@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("dockeruser", "dockerpass")},
		// docker.io supports both `/image` and `/library/image` -- make sure we
		// handle both
		{"docker.io/alpine", creds("dockeruser", "dockerpass")},
		{"docker.io/alpine:latest", creds("dockeruser", "dockerpass")},
		{"docker.io/alpine@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("dockeruser", "dockerpass")},
		{"docker.io/library/alpine:latest", creds("dockeruser", "dockerpass")},
		{"docker.io/library/alpine@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("dockeruser", "dockerpass")},
		// Non-docker registries should work as well
		{"gcr.io/foo/bar", creds("gcruser", "gcrpass")},
		{"gcr.io/foo/bar:latest", creds("gcruser", "gcrpass")},
		{"gcr.io/foo/bar@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("gcruser", "gcrpass")},
		// Subdomains should work too
		{"marketplace.gcr.io/foo/bar", creds("gcruser", "gcrpass")},
		{"marketplace.gcr.io/foo/bar:latest", creds("gcruser", "gcrpass")},
		{"marketplace.gcr.io/foo/bar@sha256:eb3e4e175ba6d212ba1d6e04fc0782916c08e1c9d7b45892e9796141b1d379ae", creds("gcruser", "gcrpass")},
	} {
		props := &platform.Properties{ContainerImage: testCase.imageRef}

		creds := container.GetPullCredentials(env, props)

		assert.Equal(
			t, testCase.expectedCredentials, creds,
			"unexpected credentials for image ref %q: %q",
			testCase.imageRef, testCase.expectedCredentials,
		)
	}
}

func creds(user, pass string) container.PullCredentials {
	return container.PullCredentials{Username: user, Password: pass}
}
