package podman_test

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/podman"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
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
	if isPodmanVersionCommand(command.GetArguments()) {
		stdio.Stdout.Write([]byte("1.0.0"))
	}
	r.commandsRun.Add(1)
	time.Sleep(100 * time.Millisecond)
	return &interfaces.CommandResult{}
}

func TestPullsNotDeduped(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	commandRunner := &SlowCommandRunner{}
	env.SetCommandRunner(commandRunner)
	dir := testfs.MakeTempDir(t)
	provider, err := podman.NewProvider(env, dir)
	require.NoError(t, err)

	props := &platform.Properties{
		ContainerImage: "docker.io/library/busybox",
		DockerNetwork:  "off",
	}
	container, err := provider.New(ctx, &container.Init{Props: props})
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

func (c *ControllableCommandRunner) Run(_ context.Context, command *repb.Command, _ string, _ func(*repb.UsageStats), stdio *interfaces.Stdio) *interfaces.CommandResult {
	if isPodmanVersionCommand(command.GetArguments()) {
		stdio.Stdout.Write([]byte("1.0.0"))
	}
	return &interfaces.CommandResult{ExitCode: *c.nextExitCode}
}

func isPodmanVersionCommand(command []string) bool {
	i1 := slices.Index(command, "podman")
	i2 := slices.Index(command, "version")
	return i1 >= 0 && i2 > i1
}

func isPodmanSubcommand(command []string, sub string) bool {
	i1 := slices.Index(command, "podman")
	i2 := slices.Index(command, sub)
	return i1 >= 0 && i2 > i1
}

type OutputCommandRunner struct{}

func (r *OutputCommandRunner) Run(_ context.Context, command *repb.Command, _ string, _ func(*repb.UsageStats), stdio *interfaces.Stdio) *interfaces.CommandResult {
	// Satisfy version probe performed by NewProvider.
	if isPodmanVersionCommand(command.GetArguments()) {
		if stdio != nil && stdio.Stdout != nil {
			stdio.Stdout.Write([]byte("1.0.0"))
		}
		return &interfaces.CommandResult{ExitCode: 0}
	}
	// Allow image pull to succeed quietly.
	if isPodmanSubcommand(command.GetArguments(), "pull") || isPodmanSubcommand(command.GetArguments(), "image") {
		return &interfaces.CommandResult{ExitCode: 0}
	}
	if isPodmanSubcommand(command.GetArguments(), "run") || isPodmanSubcommand(command.GetArguments(), "exec") {
		var err error
		if stdio != nil && stdio.Stdout != nil {
			// One big write over the limit triggers ResourceExhausted when
			// runPodman wraps the sink with the shared limit writer.
			_, err = stdio.Stdout.Write([]byte(strings.Repeat("X", 200)))
		}
		if err != nil {
			return &interfaces.CommandResult{ExitCode: -2, Error: err}
		}
		return &interfaces.CommandResult{ExitCode: 0}
	}
	return &interfaces.CommandResult{ExitCode: 0}
}

// TrackingOutputCommandRunner records podman subcommands so the test can
// verify that an overflowing exec escalates to podman rm.
type TrackingOutputCommandRunner struct {
	mu          sync.Mutex
	subcommands []string
}

func (r *TrackingOutputCommandRunner) record(subcommand string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subcommands = append(r.subcommands, subcommand)
}

func (r *TrackingOutputCommandRunner) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.subcommands...)
}

func (r *TrackingOutputCommandRunner) Run(_ context.Context, command *repb.Command, _ string, _ func(*repb.UsageStats), stdio *interfaces.Stdio) *interfaces.CommandResult {
	args := command.GetArguments()

	switch {
	case isPodmanVersionCommand(args):
		r.record("version")
		if stdio != nil && stdio.Stdout != nil {
			stdio.Stdout.Write([]byte("1.0.0"))
		}
		return &interfaces.CommandResult{ExitCode: 0}
	case isPodmanSubcommand(args, "create"):
		r.record("create")
		return &interfaces.CommandResult{ExitCode: 0}
	case isPodmanSubcommand(args, "start"):
		r.record("start")
		return &interfaces.CommandResult{ExitCode: 0}
	case isPodmanSubcommand(args, "rm"):
		r.record("rm")
		return &interfaces.CommandResult{ExitCode: 0}
	case isPodmanSubcommand(args, "exec"):
		r.record("exec")
		return &interfaces.CommandResult{
			ExitCode: -2,
			Error:    status.ResourceExhaustedError("stdout/stderr output size limit exceeded"),
		}
	default:
		return &interfaces.CommandResult{ExitCode: 0}
	}
}

func TestPodmanExec_OutputLimit(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	env.SetCommandRunner(&TrackingOutputCommandRunner{})

	buildRoot := testfs.MakeTempDir(t)
	provider, err := podman.NewProvider(env, buildRoot)
	require.NoError(t, err)

	wd := testfs.MakeDirAll(t, buildRoot, "work")
	props := &platform.Properties{ContainerImage: "docker.io/library/busybox", DockerNetwork: "off"}
	c, err := provider.New(ctx, &container.Init{Props: props})
	require.NoError(t, err)

	// Create container so we can Exec with custom stdio.
	require.NoError(t, c.Create(ctx, wd))

	res := c.Exec(ctx, &repb.Command{Arguments: []string{"echo", "hi"}}, &interfaces.Stdio{})

	require.Error(t, res.Error)
	assert.True(t, status.IsResourceExhaustedError(res.Error), "expected ResourceExhausted, got: %v", res.Error)
}

func TestPodmanExec_OutputLimit_KillsContainer(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	commandRunner := &TrackingOutputCommandRunner{}
	env.SetCommandRunner(commandRunner)

	buildRoot := testfs.MakeTempDir(t)
	provider, err := podman.NewProvider(env, buildRoot)
	require.NoError(t, err)

	wd := testfs.MakeDirAll(t, buildRoot, "work")
	props := &platform.Properties{ContainerImage: "docker.io/library/busybox", DockerNetwork: "off"}
	c, err := provider.New(ctx, &container.Init{Props: props})
	require.NoError(t, err)

	require.NoError(t, c.Create(ctx, wd))

	res := c.Exec(ctx, &repb.Command{Arguments: []string{"echo", "hi"}}, &interfaces.Stdio{})

	require.Error(t, res.Error)
	assert.True(t, status.IsResourceExhaustedError(res.Error), "expected ResourceExhausted, got: %v", res.Error)

	// podman exec itself is not sufficient here; the overflow path should also
	// remove the container so the in-container process stops running.
	subcommands := commandRunner.snapshot()
	execIndex := slices.Index(subcommands, "exec")
	rmIndex := slices.Index(subcommands, "rm")
	require.NotEqual(t, -1, execIndex, "expected podman exec to be invoked; subcommands: %v", subcommands)
	require.NotEqual(t, -1, rmIndex, "expected podman rm after exec overflow; subcommands: %v", subcommands)
	assert.Greater(t, rmIndex, execIndex, "expected podman rm after exec overflow; subcommands: %v", subcommands)
}

func TestPodmanExec_OutputLimit_UsesWrappedStdio(t *testing.T) {
	flags.Set(t, "executor.stdouterr_max_size_bytes", 10)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	env.SetCommandRunner(&OutputCommandRunner{})

	buildRoot := testfs.MakeTempDir(t)
	provider, err := podman.NewProvider(env, buildRoot)
	require.NoError(t, err)

	wd := testfs.MakeDirAll(t, buildRoot, "work")
	props := &platform.Properties{ContainerImage: "docker.io/library/busybox", DockerNetwork: "off"}
	c, err := provider.New(ctx, &container.Init{Props: props})
	require.NoError(t, err)

	require.NoError(t, c.Create(ctx, wd))

	var out bytes.Buffer
	res := c.Exec(ctx, &repb.Command{Arguments: []string{"echo", "hi"}}, &interfaces.Stdio{Stdout: &out})

	require.Error(t, res.Error)
	assert.True(t, status.IsResourceExhaustedError(res.Error), "expected ResourceExhausted, got: %v", res.Error)
}

func TestPodmanExec_DisableOutputLimits(t *testing.T) {
	flags.Set(t, "executor.stdouterr_max_size_bytes", 10)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	env.SetCommandRunner(&OutputCommandRunner{})

	buildRoot := testfs.MakeTempDir(t)
	provider, err := podman.NewProvider(env, buildRoot)
	require.NoError(t, err)

	wd := testfs.MakeDirAll(t, buildRoot, "work")
	props := &platform.Properties{ContainerImage: "docker.io/library/busybox", DockerNetwork: "off"}
	c, err := provider.New(ctx, &container.Init{Props: props})
	require.NoError(t, err)

	require.NoError(t, c.Create(ctx, wd))

	var out bytes.Buffer
	res := c.Exec(ctx, &repb.Command{Arguments: []string{"echo", "hi"}}, &interfaces.Stdio{Stdout: &out, DisableOutputLimits: true})

	require.NoError(t, res.Error)
	assert.Equal(t, 0, res.ExitCode)
	assert.GreaterOrEqual(t, out.Len(), 200)
}

func TestImageExists(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	exitCode := 0
	commandRunner := &ControllableCommandRunner{nextExitCode: &exitCode}
	env.SetCommandRunner(commandRunner)
	dir := testfs.MakeTempDir(t)

	provider, err := podman.NewProvider(env, dir)
	require.NoError(t, err)

	props := &platform.Properties{
		ContainerImage: "docker.io/library/busybox",
		DockerNetwork:  "off",
	}
	container, err := provider.New(ctx, &container.Init{Props: props})
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
