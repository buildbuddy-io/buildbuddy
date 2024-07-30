package container_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/testing/protocmp"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type FakeContainer struct {
	RequiredPullCredentials oci.Credentials
	PullCount               int
	pullDelay               time.Duration
}

func (c *FakeContainer) IsolationType() string {
	return "fake"
}
func (c *FakeContainer) Run(context.Context, *repb.Command, string, oci.Credentials) *interfaces.CommandResult {
	return nil
}
func (c *FakeContainer) IsImageCached(context.Context) (bool, error) {
	return c.PullCount > 0, nil
}
func (c *FakeContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	if c.pullDelay > 0*time.Second {
		time.Sleep(c.pullDelay)
	}
	if creds != c.RequiredPullCredentials {
		return status.PermissionDeniedError("Permission denied: wrong pull credentials")
	}
	c.PullCount++
	return nil
}
func (c *FakeContainer) Create(context.Context, string) error { return nil }
func (c *FakeContainer) Exec(context.Context, *repb.Command, *interfaces.Stdio) *interfaces.CommandResult {
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

func TestPullImageIfNecessary_ParallelCallsSerialized(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1", "US2", "GR2"))
	env.SetAuthenticator(ta)
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))
	imageRef := "docker.io/some-org/some-image:v1.0.0"
	ctx := userCtx(t, ta, "US1")
	c := &FakeContainer{pullDelay: time.Second}

	assert.Equal(t, 0, c.PullCount, "sanity check: pull count should be 0 initially")
	eg := errgroup.Group{}
	for i := 0; i < 20; i++ {
		eg.Go(func() error { return container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, imageRef) })
	}
	require.NoError(t, eg.Wait())
	assert.Equal(t, 1, c.PullCount, "image should only be pulled once")
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

func TestUsageStats(t *testing.T) {
	s := &container.UsageStats{}
	require.Empty(t, cmp.Diff(&repb.UsageStats{}, s.TaskStats(), protocmp.Transform()))

	s.SetBaseline(&repb.UsageStats{})
	require.Empty(t, cmp.Diff(&repb.UsageStats{}, s.TaskStats(), protocmp.Transform()))

	// Observe some cgroup usage.
	s.Update(&repb.UsageStats{
		CpuNanos:       1e9,
		MemoryBytes:    50 * 1024 * 1024,
		CpuPressure:    makePSI(100, 10),
		MemoryPressure: makePSI(2_000, 200),
		IoPressure:     makePSI(30_000, 3_000),
	})

	// Observe the same usage again but with higher memory.
	s.Update(&repb.UsageStats{
		CpuNanos:       1e9,
		MemoryBytes:    55 * 1024 * 1024,
		CpuPressure:    makePSI(100, 10),
		MemoryPressure: makePSI(2_000, 200),
		IoPressure:     makePSI(30_000, 3_000),
	})

	// Observe the same usage again but with lower memory than the first
	// observation. Also, accumulate some CPU as well as some pressure stall
	// time.
	s.Update(&repb.UsageStats{
		CpuNanos:       2e9,
		MemoryBytes:    45 * 1024 * 1024,
		CpuPressure:    makePSI(150, 10),
		MemoryPressure: makePSI(2_000, 200),
		IoPressure:     makePSI(30_000, 3_000),
	})
	require.Empty(t, cmp.Diff(&repb.UsageStats{
		CpuNanos:        2e9,
		MemoryBytes:     45 * 1024 * 1024,
		PeakMemoryBytes: 55 * 1024 * 1024,
		CpuPressure:     makePSI(150, 10),
		MemoryPressure:  makePSI(2_000, 200),
		IoPressure:      makePSI(30_000, 3_000),
	}, s.TaskStats(), protocmp.Transform()))

	// Start a new task, setting the baseline to the last observed stats. The
	// cgroup accumulated CPU etc. will not be reset, but the metrics that we
	// report should appear as though they were.
	s.SetBaseline(&repb.UsageStats{
		CpuNanos:       2e9,
		MemoryBytes:    45 * 1024 * 1024,
		CpuPressure:    makePSI(150, 10),
		MemoryPressure: makePSI(2_000, 200),
		IoPressure:     makePSI(30_000, 3_000),
	})
	require.Empty(t, cmp.Diff(&repb.UsageStats{
		CpuPressure:    makePSI(0, 0),
		MemoryPressure: makePSI(0, 0),
		IoPressure:     makePSI(0, 0),
	}, s.TaskStats(), protocmp.Transform()))

	// Re-observe the last observation again. The only usage that should be
	// reported is memory, since all other usage should be relative to the last
	// observation from the previous task.
	s.Update(&repb.UsageStats{
		CpuNanos:       2e9,
		MemoryBytes:    45 * 1024 * 1024,
		CpuPressure:    makePSI(150, 10),
		MemoryPressure: makePSI(2_000, 200),
		IoPressure:     makePSI(30_000, 3_000),
	})
	require.Empty(t, cmp.Diff(&repb.UsageStats{
		MemoryBytes:     45 * 1024 * 1024,
		PeakMemoryBytes: 45 * 1024 * 1024,
		CpuPressure:     makePSI(0, 0),
		MemoryPressure:  makePSI(0, 0),
		IoPressure:      makePSI(0, 0),
	}, s.TaskStats(), protocmp.Transform()))

	// Accumulate some CPU usage as well as some pressure stall time. Task stats
	// should only reflect the accumulated amount.
	s.Update(&repb.UsageStats{
		CpuNanos:       2e9 + 7e9,
		MemoryBytes:    45 * 1024 * 1024,
		CpuPressure:    makePSI(150+117, 10+17),
		MemoryPressure: makePSI(2_000+1_118, 200+118),
		IoPressure:     makePSI(30_000+11_119, 3_000+1_119),
	})
	require.Empty(t, cmp.Diff(&repb.UsageStats{
		CpuNanos:        7e9,
		MemoryBytes:     45 * 1024 * 1024,
		PeakMemoryBytes: 45 * 1024 * 1024,
		CpuPressure:     makePSI(117, 17),
		MemoryPressure:  makePSI(1_118, 118),
		IoPressure:      makePSI(11_119, 1_119),
	}, s.TaskStats(), protocmp.Transform()))
}

func makePSI(someTotal, fullTotal int64) *repb.PSI {
	return &repb.PSI{
		Some: &repb.PSI_Metrics{
			Total: someTotal,
		},
		Full: &repb.PSI_Metrics{
			Total: fullTotal,
		},
	}
}
