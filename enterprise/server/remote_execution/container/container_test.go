package container_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type FakeContainer struct {
	RequiredPullCredentials container.PullCredentials
	PullCount               int
}

func (c *FakeContainer) Run(context.Context, *repb.Command, string, container.PullCredentials) *interfaces.CommandResult {
	return nil
}
func (c *FakeContainer) IsImageCached(context.Context) (bool, error) {
	return c.PullCount > 0, nil
}
func (c *FakeContainer) PullImage(ctx context.Context, creds container.PullCredentials) error {
	if creds != c.RequiredPullCredentials {
		return status.PermissionDeniedError("Permission denied: wrong pull credentials")
	}
	c.PullCount++
	return nil
}
func (c *FakeContainer) Create(context.Context, string) error { return nil }
func (c *FakeContainer) Exec(context.Context, *repb.Command, *container.Stdio) *interfaces.CommandResult {
	return nil
}
func (c *FakeContainer) Remove(ctx context.Context) error  { return nil }
func (c *FakeContainer) Pause(ctx context.Context) error   { return nil }
func (c *FakeContainer) Unpause(ctx context.Context) error { return nil }
func (c *FakeContainer) Stats(context.Context) (*repb.UsageStats, error) {
	return &repb.UsageStats{}, nil
}

func userCtx(t *testing.T, ta *testauth.TestAuthenticator, userID string) context.Context {
	ctx, err := ta.WithAuthenticatedUser(context.Background(), userID)
	require.NoError(t, err)
	return ctx
}

func TestPullImageIfNecessary_ValidCredentials(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1", "US2", "GR2"))
	env.SetAuthenticator(ta)
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	imageRef := "docker.io/some-org/some-image:v1.0.0"
	ctx := userCtx(t, ta, "US1")
	goodCreds1 := container.PullCredentials{
		Username: "user",
		Password: "short-lived-token-1",
	}
	goodCreds2 := container.PullCredentials{
		Username: "user",
		Password: "short-lived-token-2",
	}
	c := &FakeContainer{RequiredPullCredentials: goodCreds1}

	assert.Equal(t, 0, c.PullCount, "sanity check: pull count should be 0 initially")

	err := container.PullImageIfNecessary(ctx, env, cacheAuth, c, goodCreds1, imageRef)

	require.NoError(t, err)
	assert.Equal(t, 1, c.PullCount, "should pull the image if credentials are valid")

	err = container.PullImageIfNecessary(ctx, env, cacheAuth, c, goodCreds1, imageRef)

	require.NoError(t, err)
	assert.Equal(t, 1, c.PullCount, "should not need to immediately re-authenticate with the remote registry")

	err = container.PullImageIfNecessary(ctx, env, cacheAuth, c, goodCreds2, imageRef)

	require.NoError(t, err)
	assert.Equal(
		t, 1, c.PullCount,
		"should not need to immediately re-authenticate even if a different short-lived token is provided, since the group just pulled the image")
}

func TestPullImageIfNecessary_InvalidCredentials_PermissionDenied(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1", "US2", "GR2"))
	env.SetAuthenticator(ta)
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	imageRef := "docker.io/some-org/some-image:v1.0.0"
	ctx := userCtx(t, ta, "US1")
	goodCreds := container.PullCredentials{
		Username: "user",
		Password: "secret",
	}
	c := &FakeContainer{RequiredPullCredentials: goodCreds}
	badCreds := container.PullCredentials{
		Username: "user",
		Password: "trying-to-guess-the-real-secret",
	}

	err := container.PullImageIfNecessary(ctx, env, cacheAuth, c, badCreds, imageRef)

	require.True(t, status.IsPermissionDeniedError(err), "should return PermissionDenied if credentials are valid")

	err = container.PullImageIfNecessary(ctx, env, cacheAuth, c, badCreds, imageRef)

	require.True(t, status.IsPermissionDeniedError(err), "should return PermissionDenied on subsequent attempts as well")

	err = container.PullImageIfNecessary(ctx, env, cacheAuth, c, goodCreds, imageRef)

	require.NoError(t, err, "good creds should still work after previous incorrect attempts")
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
	flags.Set(
		t,
		"executor.container_registries",
		[]container.ContainerRegistry{
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
		},
	)

	props := &platform.Properties{
		ContainerImage:            "missing-password.io",
		ContainerRegistryUsername: "username",
		ContainerRegistryPassword: "",
	}
	_, err := container.GetPullCredentials(env, props)
	assert.True(t, status.IsInvalidArgumentError(err))

	props = &platform.Properties{
		ContainerImage:            "missing-username.io",
		ContainerRegistryUsername: "",
		ContainerRegistryPassword: "password",
	}
	_, err = container.GetPullCredentials(env, props)
	assert.True(t, status.IsInvalidArgumentError(err))

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
		props = &platform.Properties{ContainerImage: testCase.imageRef}

		creds, err := container.GetPullCredentials(env, props)

		assert.NoError(t, err)
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
