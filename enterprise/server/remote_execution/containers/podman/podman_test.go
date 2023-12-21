package podman_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/podman"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type SlowCommandRunner struct {
	commandsRun *atomic.Int32
}

func (r SlowCommandRunner) Run(_ context.Context, _ *repb.Command, _ string, _ func(*repb.UsageStats), _ *interfaces.Stdio) *interfaces.CommandResult {
	r.commandsRun.Add(1)
	time.Sleep(100 * time.Millisecond)
	return &interfaces.CommandResult{}
}

func TestPullsNotDeduped(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	commandsRun := atomic.Int32{}
	commandRunner := SlowCommandRunner{commandsRun: &commandsRun}
	env.SetCommandRunner(commandRunner)
	dir := testfs.MakeTempDir(t)
	provider, err := podman.NewProvider(env, dir)
	require.NoError(t, err)

	props := platform.Properties{
		ContainerImage: "docker.io/library/busybox",
		DockerNetwork:  "off",
	}
	container, err := provider.New(ctx, &props, nil, nil, "")
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		go require.NoError(t, container.PullImage(ctx, oci.Credentials{}))
	}
	require.Equal(t, int32(5), commandsRun.Load())
}

type ControllableCommandRunner struct {
	nextExitCode *int
}

func (c ControllableCommandRunner) Run(_ context.Context, _ *repb.Command, _ string, _ func(*repb.UsageStats), _ *interfaces.Stdio) *interfaces.CommandResult {
	return &interfaces.CommandResult{ExitCode: *c.nextExitCode}
}

func TestImageExists(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	exitCode := 0
	commandRunner := ControllableCommandRunner{nextExitCode: &exitCode}
	env.SetCommandRunner(commandRunner)
	dir := testfs.MakeTempDir(t)
	provider, err := podman.NewProvider(env, dir)
	require.NoError(t, err)

	props := platform.Properties{
		ContainerImage: "docker.io/library/busybox",
		DockerNetwork:  "off",
	}
	container, err := provider.New(ctx, &props, nil, nil, "")
	require.NoError(t, err)

	exitCode = 1
	cached, err := container.IsImageCached(ctx)
	require.NoError(t, err)
	assert.False(t, cached)

	exitCode = 0
	cached, err = container.IsImageCached(ctx)
	require.NoError(t, err)
	assert.True(t, cached)

	// Image existence should be cached, so returning 1 should have no effect.
	exitCode = 1
	cached, err = container.IsImageCached(ctx)
	require.NoError(t, err)
	assert.True(t, cached)
}
