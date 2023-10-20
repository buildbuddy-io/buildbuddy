package container_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

type FakeContainer struct {
	RequiredPullCredentials oci.Credentials
	PullCount               int
}

func (c *FakeContainer) Run(context.Context, *repb.Command, string, oci.Credentials) *interfaces.CommandResult {
	return nil
}
func (c *FakeContainer) IsImageCached(context.Context) (bool, error) {
	return c.PullCount > 0, nil
}
func (c *FakeContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	if creds != c.RequiredPullCredentials {
		return status.PermissionDeniedError("Permission denied: wrong pull credentials")
	}
	c.PullCount++
	return nil
}
func (c *FakeContainer) Create(context.Context, string) error { return nil }
func (c *FakeContainer) Exec(context.Context, *repb.Command, *commandutil.Stdio) *interfaces.CommandResult {
	return nil
}
func (c *FakeContainer) Remove(ctx context.Context) error  { return nil }
func (c *FakeContainer) Pause(ctx context.Context) error   { return nil }
func (c *FakeContainer) Unpause(ctx context.Context) error { return nil }
func (c *FakeContainer) Stats(context.Context) (*repb.UsageStats, error) {
	return &repb.UsageStats{}, nil
}
func (c *FakeContainer) State(ctx context.Context) (*rnpb.ContainerState, error) {
	return nil, status.UnimplementedError("not implemented")
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
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))
	imageRef := "docker.io/some-org/some-image:v1.0.0"
	ctx := userCtx(t, ta, "US1")
	goodCreds1 := oci.Credentials{
		Username: "user",
		Password: "short-lived-token-1",
	}
	goodCreds2 := oci.Credentials{
		Username: "user",
		Password: "short-lived-token-2",
	}
	c := &FakeContainer{RequiredPullCredentials: goodCreds1}

	assert.Equal(t, 0, c.PullCount, "sanity check: pull count should be 0 initially")

	err := container.PullImageIfNecessary(ctx, env, c, goodCreds1, imageRef)

	require.NoError(t, err)
	assert.Equal(t, 1, c.PullCount, "should pull the image if credentials are valid")

	err = container.PullImageIfNecessary(ctx, env, c, goodCreds1, imageRef)

	require.NoError(t, err)
	assert.Equal(t, 1, c.PullCount, "should not need to immediately re-authenticate with the remote registry")

	err = container.PullImageIfNecessary(ctx, env, c, goodCreds2, imageRef)

	require.NoError(t, err)
	assert.Equal(
		t, 1, c.PullCount,
		"should not need to immediately re-authenticate even if a different short-lived token is provided, since the group just pulled the image")
}

func TestPullImageIfNecessary_InvalidCredentials_PermissionDenied(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1", "US2", "GR2"))
	env.SetAuthenticator(ta)
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))
	imageRef := "docker.io/some-org/some-image:v1.0.0"
	ctx := userCtx(t, ta, "US1")
	goodCreds := oci.Credentials{
		Username: "user",
		Password: "secret",
	}
	c := &FakeContainer{RequiredPullCredentials: goodCreds}
	badCreds := oci.Credentials{
		Username: "user",
		Password: "trying-to-guess-the-real-secret",
	}

	err := container.PullImageIfNecessary(ctx, env, c, badCreds, imageRef)

	require.True(t, status.IsPermissionDeniedError(err), "should return PermissionDenied if credentials are valid")

	err = container.PullImageIfNecessary(ctx, env, c, badCreds, imageRef)

	require.True(t, status.IsPermissionDeniedError(err), "should return PermissionDenied on subsequent attempts as well")

	err = container.PullImageIfNecessary(ctx, env, c, goodCreds, imageRef)

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
			oci.Credentials{Username: "user1", Password: "pass1"},
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
			oci.Credentials{Username: "user1", Password: "pass1"},
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
			oci.Credentials{Username: "user1", Password: "pass1"},
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
			oci.Credentials{},
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
			oci.Credentials{},
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
			oci.Credentials{Username: "user1", Password: "pass1"},
			"gcr.io/org1/image:latest",
		)

		require.NoError(t, err)

		auth.Refresh(immediatelyExpiredToken)
		isAuthorized := auth.IsAuthorized(immediatelyExpiredToken)

		assert.False(t, isAuthorized, "Expired tokens should not be authorized")
	}
}
