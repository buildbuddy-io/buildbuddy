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
	"golang.org/x/sync/errgroup"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type SlowCommandRunner struct {
	commandsRun atomic.Int32
}

func (r *SlowCommandRunner) Run(_ context.Context, command *repb.Command, _ string, _ func(*repb.UsageStats), stdio *interfaces.Stdio) *interfaces.CommandResult {
	if len(command.Arguments) >= 2 && command.Arguments[0] == "podman" && command.Arguments[1] == "version" {
		stdio.Stdout.Write([]byte("1.0.0"))
	}
	r.commandsRun.Add(1)
	time.Sleep(100 * time.Millisecond)
	return &interfaces.CommandResult{}
}

func TestPullsNotDeduped(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	commandRunner := SlowCommandRunner{}
	env.SetCommandRunner(&commandRunner)
	dir := testfs.MakeTempDir(t)
	provider, err := podman.NewProvider(env, dir)
	require.NoError(t, err)

	props := platform.Properties{
		ContainerImage: "docker.io/library/busybox",
		DockerNetwork:  "off",
	}
	container, err := provider.New(ctx, &props, nil, nil, "")
	require.NoError(t, err)

	eg := errgroup.Group{}
	for i := 0; i < 5; i++ {
		eg.Go(func() error {
			return container.PullImage(ctx, oci.Credentials{})
		})
	}
	require.NoError(t, eg.Wait())

	// One extra command for `podman version`
	require.Equal(t, int32(6), commandRunner.commandsRun.Load())
}

type ControllableCommandRunner struct {
	nextExitCode *int
}

func (c ControllableCommandRunner) Run(_ context.Context, command *repb.Command, _ string, _ func(*repb.UsageStats), stdio *interfaces.Stdio) *interfaces.CommandResult {
	if len(command.Arguments) >= 2 && command.Arguments[0] == "podman" && command.Arguments[1] == "version" {
		stdio.Stdout.Write([]byte("1.0.0"))
	}
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
