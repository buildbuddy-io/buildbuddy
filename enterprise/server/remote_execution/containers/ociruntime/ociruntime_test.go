package ociruntime_test

import (
	"archive/tar"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io/fs"
	"log"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/persistentworker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cpuset"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/quarantine"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testnetworking"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testtar"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	wkpb "github.com/buildbuddy-io/buildbuddy/proto/worker"
	containerregistry "github.com/google/go-containerregistry/pkg/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// Set via x_defs in BUILD file.
var (
	crunRlocationpath       string
	busyboxRlocationpath    string
	ociBusyboxRlocationpath string
	testworkerRlocationpath string
	netToolsImageRef        string
)

func init() {
	runtimePath, err := runfiles.Rlocation(crunRlocationpath)
	if err != nil {
		log.Fatalf("Failed to locate crun in runfiles: %s", err)
	}
	*ociruntime.Runtime = runtimePath
}

func setupNetworking(t *testing.T) {
	err := networking.Configure(context.Background())
	require.NoError(t, err)
	testnetworking.Setup(t)
	// Disable network pooling in tests to simplify cleanup.
	flags.Set(t, "executor.oci.network_pool_size", 0)
}

// Returns a special image ref indicating that a busybox-based rootfs should
// be provisioned using the 'busybox' binary on the host machine.
// Skips the test if busybox is not available.
// TODO: use a static test binary + empty rootfs, instead of relying on busybox
// for testing
func manuallyProvisionedBusyboxImage(t *testing.T) string {
	// Make sure our bazel-provisioned busybox binary is in PATH.
	busyboxPath, err := runfiles.Rlocation(busyboxRlocationpath)
	require.NoError(t, err)
	if path, _ := exec.LookPath("busybox"); path != busyboxPath {
		err := os.Setenv("PATH", filepath.Dir(busyboxPath)+":"+os.Getenv("PATH"))
		require.NoError(t, err)
	}
	return ociruntime.TestBusyboxImageRef
}

// Returns an image ref pointing to a remotely hosted busybox image.
// Skips the test if we don't have mount permissions.
// TODO: support rootless overlayfs mounts and get rid of this
func realBusyboxImage(t *testing.T) string {
	if !hasMountPermissions(t) {
		t.Skipf("using a real container image with overlayfs requires mount permissions")
	}
	return "mirror.gcr.io/library/busybox"
}

func netToolsImage(t *testing.T) string {
	if !hasMountPermissions(t) {
		t.Skipf("using a real container image with overlayfs requires mount permissions")
	}
	return netToolsImageRef
}

// Returns a remote reference to the image in //dockerfiles/test_images/ociruntime_test/image_config_test_image
func imageConfigTestImage(t *testing.T) string {
	if !hasMountPermissions(t) {
		t.Skipf("using a real container image with overlayfs requires mount permissions")
	}
	return "gcr.io/flame-public/image-config-test@sha256:44dc4623f3709eef89b0a6d6c8e1c3a9d54db73f6beb8cf99f402052ba9abe56"
}

func installLeaserInEnv(t testing.TB, env *real_environment.RealEnv) {
	leaser, err := cpuset.NewLeaser(cpuset.LeaserOpts{})
	require.NoError(t, err)
	env.SetCPULeaser(leaser)
	flags.Set(t, "executor.cpu_leaser.enable", true)

	t.Cleanup(func() {
		orphanedLeases := leaser.TestOnlyGetOpenLeases()
		require.Equal(t, 0, len(orphanedLeases))
	})
}

func TestRun(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")
	testfs.WriteAllFileContents(t, wd, map[string]string{
		"input.txt": "world",
	})

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	// Run
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			echo "$GREETING $(cat input.txt)!"
			touch output.txt
		`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Equal(t, "Hello world!\n", string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, 0, res.ExitCode)
	assert.True(t, testfs.Exists(t, wd, "output.txt"), "output.txt should exist")
}

func TestCgroupSettings(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{
		Task: &repb.ScheduledTask{
			SchedulingMetadata: &scpb.SchedulingMetadata{
				CgroupSettings: &scpb.CgroupSettings{
					CpuQuotaLimitUsec:  proto.Int64(300000),
					CpuQuotaPeriodUsec: proto.Int64(100000),
					PidsMax:            proto.Int64(256),
				},
			},
		},
		Props: &platform.Properties{
			ContainerImage: image,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	// Run
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			cat /sys/fs/cgroup/cpu.max
			cat /sys/fs/cgroup/pids.max
		`},
	}

	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Equal(t, "300000 100000\n256\n", string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, 0, res.ExitCode)
}

func TestRunUsageStats(t *testing.T) {
	setupNetworking(t)

	image := realBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	// Run (sleep long enough to collect stats)
	// TODO: in the Run case, we should be able to use the memory.peak file and
	// cumulative CPU usage file to reliably return stats even if we don't have
	// a chance to poll
	cmd := &repb.Command{Arguments: []string{"sleep", "0.5"}}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	require.Equal(t, 0, res.ExitCode)
	assert.Greater(t, res.UsageStats.GetPeakMemoryBytes(), int64(0), "memory")
	assert.Greater(t, res.UsageStats.GetCpuNanos(), int64(0), "CPU")
}

func TestRunWithImage(t *testing.T) {
	setupNetworking(t)

	image := imageConfigTestImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	// Run
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			echo "$GREETING world!"
			env | sort
		`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Equal(t, `Hello world!
GREETING=Hello
HOME=/home/buildbuddy
HOSTNAME=localhost
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/test/bin
PWD=/buildbuddy-execroot
SHLVL=1
TEST_ENV_VAR=foo
`, string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, 0, res.ExitCode)
}

func TestRunOOM(t *testing.T) {
	setupNetworking(t)

	image := realBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)
	// Enable CAP_SYS_RESOURCE just for this test so that we can write to
	// oom_score_adj. Otherwise it's hard to guarantee that our top-level shell
	// process doesn't get killed, because we're at the whim of the OOM killer.
	flags.Set(t, "executor.oci.cap_add", []string{"CAP_SYS_RESOURCE"})

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{
		Task: &repb.ScheduledTask{
			SchedulingMetadata: &scpb.SchedulingMetadata{
				CgroupSettings: &scpb.CgroupSettings{
					// Set a relatively small memory limit to trigger an OOM.
					MemoryLimitBytes: proto.Int64(8_000_000),
					// Eagerly kill the whole container on OOM - errors are
					// returned on OOM anyway and the task is retried, so there
					// is no need to continue once an OOM has occurred.
					MemoryOomGroup: proto.Bool(true),
				},
			},
		},
		Props: &platform.Properties{
			ContainerImage: image,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	cmd := &repb.Command{Arguments: []string{"sh", "-c", `

# Make this top-level shell process extremely unlikely to be killed by the OOM
# killer by setting a score of -999. We don't go all the way to -1000, since
# that is equivalent to disabling the OOM killer completely, according to
# 'man 5 proc_pid_oom_score_adj'.
#
# Note, we need CAP_SYS_RESOURCE to decrease our OOM score.

echo -999 > /proc/$$/oom_score_adj

# Start several child processes in the background, which each increase their
# OOM score so that they are more likely to be targeted by the OOM killer, then
# perform lots of memory allocations until they are killed.
#
# Note, CAP_SYS_RESOURCE is not needed to increase the OOM score; it's only
# needed to decrease it.

for _ in $(seq 1 4); do
	sh -ec '
		echo 1000 > /proc/$$/oom_score_adj;
		STR=A; while true; do STR="${STR}${STR}"; done
	' &
done

# Wait for all child shells to terminate.
# Since we're setting memory.oom.group=true, we shouldn't actually get past
# this statement.
wait

# Run a few commands - if we actually observe their output,
# the test should fail, since we shouldn't be getting this far.

echo "All child processes were killed!"
cat /sys/fs/cgroup/memory.events

`}}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	assert.True(t, status.IsUnavailableError(res.Error), "expected UnavailableError, got %#+v", res.Error)
	assert.Equal(t, "task process or child process killed by oom killer", status.Message(res.Error))
	assert.Empty(t, string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
}

func TestCreateExecRemove(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)

	// Create
	require.NoError(t, err)
	err = c.Create(ctx, wd)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = c.Remove(ctx)
		require.NoError(t, err)
	})

	// Exec
	cmd := &repb.Command{Arguments: []string{"sh", "-c", "cat && pwd"}}
	stdio := interfaces.Stdio{
		Stdin: strings.NewReader("buildbuddy was here: "),
	}
	res := c.Exec(ctx, cmd, &stdio)
	require.NoError(t, res.Error)

	assert.Equal(t, 0, res.ExitCode)
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, "buildbuddy was here: /buildbuddy-execroot\n", string(res.Stdout))
}

func TestTini_Run(t *testing.T) {
	setupNetworking(t)

	image := realBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)
	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")
	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
		DockerInit:     true, // enable tini
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err = c.Remove(ctx)
		require.NoError(t, err)
	})

	// For now just check that tini is pid 1. It's difficult to write a shell
	// script that intentionally creates a zombie process, since shells will
	// handle SIGCHLD and reap processes.
	cmd := &repb.Command{Arguments: []string{"cat", "/proc/1/stat"}}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Empty(t, string(res.Stderr))
	assert.True(t, strings.HasPrefix(string(res.Stdout), "1 (tini)"), "tini should be pid 1. /proc/1/stat contents: %q", string(res.Stdout))
	assert.Equal(t, 0, res.ExitCode)
}

func TestTini_CreateExec(t *testing.T) {
	setupNetworking(t)

	image := realBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)
	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
		DockerInit:     true, // enable tini
	}})
	require.NoError(t, err)

	// Pull
	require.NoError(t, err)
	err = c.PullImage(ctx, oci.Credentials{})
	require.NoError(t, err)

	// Create
	require.NoError(t, err)
	err = c.Create(ctx, wd)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = c.Remove(ctx)
		require.NoError(t, err)
	})

	// Exec 1: create an orphaned process that writes its PID to a pipe
	// and waits for it to be read, then exits.
	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
		mkfifo pid.pipe
		sh -c 'echo $$ >> pid.pipe' &>/dev/null &
	`}}
	res := c.Exec(ctx, cmd, &interfaces.Stdio{})
	require.NoError(t, res.Error)
	require.Empty(t, string(res.Stderr))
	require.Equal(t, 0, res.ExitCode)

	// Exec 2: read the PID from the pipe and wait for the process to be reaped.
	// If it is never reaped then the test will time out.
	cmd = &repb.Command{Arguments: []string{"sh", "-c", `
		read PID < pid.pipe
		while [ -e /proc/$PID/stat ]; do
			sleep 0.1
		done
	`}}
	res = c.Exec(ctx, cmd, &interfaces.Stdio{})
	require.NoError(t, res.Error)

	assert.Equal(t, 0, res.ExitCode)
	assert.Empty(t, string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
}

func TestExecUsageStats(t *testing.T) {
	setupNetworking(t)

	image := realBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	err = c.PullImage(ctx, oci.Credentials{})
	require.NoError(t, err)
	err = c.Create(ctx, wd)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = c.Remove(ctx)
		require.NoError(t, err)
	})

	// Exec
	cmd := &repb.Command{Arguments: []string{"sleep", "0.5"}}
	res := c.Exec(ctx, cmd, &interfaces.Stdio{})
	require.NoError(t, res.Error)
	require.Equal(t, 0, res.ExitCode)
	assert.Greater(t, res.UsageStats.GetMemoryBytes(), int64(0), "memory")
	assert.Greater(t, res.UsageStats.GetCpuNanos(), int64(0), "CPU")

	// Stats
	s, err := c.Stats(ctx)
	require.NoError(t, err)
	assert.Greater(t, s.GetMemoryBytes(), int64(0), "memory")
	assert.Greater(t, s.GetCpuNanos(), int64(0), "CPU")
}

func TestStatsPostExec(t *testing.T) {
	setupNetworking(t)

	image := realBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})
	err = c.PullImage(ctx, oci.Credentials{})
	require.NoError(t, err)
	err = c.Create(ctx, wd)
	require.NoError(t, err)

	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
		# Use 32MB of memory (in /dev/shm).
		# Using this approach instead of allocating a big string,
		# because it seems like 'sh' takes a while to actually free the string,
		# but for what we're trying to test here, we need the memory to be freed
		# immediately (before it can be caught by our stats polling).
		cat /dev/zero | head -c 32000000 > /dev/shm/FILE

		# Hold the memory long enough for the stats polling to register it.
		sleep 0.25

		# Free the memory then immediately exit.
		rm /dev/shm/FILE
	`}}
	res := c.Exec(ctx, cmd, &interfaces.Stdio{})
	require.NoError(t, res.Error)
	assert.Empty(t, string(res.Stderr))
	require.Equal(t, 0, res.ExitCode)

	// Stats() should report ~0 memory usage since the task has completed,
	// even though it used 32MB of memory.
	stats, err := c.Stats(ctx)
	require.NoError(t, err)
	require.Less(t, stats.GetMemoryBytes(), int64(2e6), "final memory usage should be much less than 32MB")
}

func TestPullCreateExecRemove(t *testing.T) {
	setupNetworking(t)

	image := imageConfigTestImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage: image,
		},
	})
	require.NoError(t, err)

	// Pull
	err = c.PullImage(ctx, oci.Credentials{})
	require.NoError(t, err)

	// Ensure cached
	cached, err := c.IsImageCached(ctx)
	require.NoError(t, err)
	assert.True(t, cached, "IsImageCached")

	// Create
	require.NoError(t, err)
	err = c.Create(ctx, wd)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = c.Remove(ctx)
		require.NoError(t, err)
	})

	// Exec
	cmd := &repb.Command{
		Arguments: []string{"sh", "-ec", `
			touch /bin/foo.txt
			pwd
			env | sort
		`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	stdio := interfaces.Stdio{}
	res := c.Exec(ctx, cmd, &stdio)
	require.NoError(t, res.Error)

	assert.Equal(t, 0, res.ExitCode)
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, `/buildbuddy-execroot
GREETING=Hello
HOME=/root
HOSTNAME=localhost
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/test/bin
PWD=/buildbuddy-execroot
SHLVL=1
TEST_ENV_VAR=foo
`, string(res.Stdout))

	// Make sure that no cached images were modified.
	layersRoot := filepath.Join(cacheRoot, "images", "oci")
	err = filepath.WalkDir(layersRoot, func(path string, entry fs.DirEntry, err error) error {
		require.NoError(t, err)
		assert.NotEqual(t, entry.Name(), "foo.txt")
		return nil
	})
	require.NoError(t, err)
}

func TestCreateExecPauseUnpause(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)

	// Create
	require.NoError(t, err)
	err = c.Create(ctx, wd)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = c.Remove(ctx)
		require.NoError(t, err)
	})

	// Exec: start a bg process that increments a counter file every 10ms.
	const updateInterval = 10 * time.Millisecond
	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
		printf 0 > count.txt
		(
			count=0
			while true; do
				count=$((count+1))
				printf '%d' "$count" > count.txt
				sleep ` + fmt.Sprintf("%f", updateInterval.Seconds()) + `
			done
		) &
	`}}
	res := c.Exec(ctx, cmd, &interfaces.Stdio{})
	require.NoError(t, res.Error)

	t.Logf("Started update process in container")

	assert.Empty(t, string(res.Stderr))
	assert.Empty(t, string(res.Stdout))
	require.Equal(t, 0, res.ExitCode)

	readCounterFile := func() int {
		for {
			b, err := os.ReadFile(filepath.Join(wd, "count.txt"))
			require.NoError(t, err)
			s := string(b)
			if s == "" {
				// File is being written concurrently; retry
				continue
			}
			c, err := strconv.Atoi(s)
			require.NoError(t, err)
			return c
		}
	}

	waitUntilCounterIncremented := func() {
		var lastCount *int
		for {
			count := readCounterFile()
			if lastCount != nil && count > *lastCount {
				return
			}
			lastCount = &count
			time.Sleep(updateInterval)
		}
	}

	// Counter should be getting continually updated since we started a
	// background process in the container and we haven't paused yet.
	waitUntilCounterIncremented()

	// Pause
	err = c.Pause(ctx)
	require.NoError(t, err)

	// Counter should not be getting updated anymore since we're paused.
	d1 := readCounterFile()
	require.NotEmpty(t, d1)
	time.Sleep(updateInterval * 10)
	d2 := readCounterFile()
	require.Equal(t, d1, d2, "expected counter file not to be updated")

	// Unpause
	err = c.Unpause(ctx)
	require.NoError(t, err)

	// Counter should be getting continually updated again since we're unpaused.
	waitUntilCounterIncremented()
}

func TestCreateFailureHasStderr(t *testing.T) {
	quarantine.SkipQuarantinedTest(t)
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	// Create
	c, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage: image,
		},
	})
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	require.NoError(t, err)
	err = c.Create(ctx, wd+"nonexistent")
	require.ErrorContains(t, err, "nonexistent")
}

func TestDevices(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	// Create
	c, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage: image,
		},
	})

	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})
	require.NoError(t, err)
	res := c.Run(ctx, &repb.Command{
		Arguments: []string{"sh", "-e", "-c", `
			# Print out device file types and major/minor device numbers
			stat -c '%n: %F (%t,%T)' /dev/null /dev/zero /dev/random /dev/urandom

			# Perform a bit of device I/O
			cat /dev/random | head -c1 >/dev/null
			cat /dev/urandom | head -c1 >/dev/null
			cat /dev/zero | head -c1 >/dev/null
			echo foo >/dev/null
		`},
	}, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Equal(t, 0, res.ExitCode)
	expectedLines := []string{
		"/dev/null: character special file (1,3)",
		"/dev/zero: character special file (1,5)",
		"/dev/random: character special file (1,8)",
		"/dev/urandom: character special file (1,9)",
	}
	assert.Equal(t, strings.Join(expectedLines, "\n")+"\n", string(res.Stdout))
	assert.Equal(t, "", string(res.Stderr))
}

func TestSignal(t *testing.T) {
	quarantine.SkipQuarantinedTest(t)
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
		trap 'echo "Got SIGTERM" && touch .SIGNALED && exit 1' TERM
		touch .STARTED
		sleep 999999999
	`}}

	go func() {
		// Wait for command to start
		err := disk.WaitUntilExists(ctx, filepath.Join(wd, ".STARTED"), disk.WaitOpts{Timeout: -1})
		require.NoError(t, err)
		// Send SIGTERM
		err = c.Signal(ctx, syscall.SIGTERM)
		require.NoError(t, err)
	}()

	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	assert.NoError(t, res.Error)
	assert.Equal(t, "Got SIGTERM\n", string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
}

func TestNetwork_Enabled(t *testing.T) {
	setupNetworking(t)

	// Note: busybox has ping, but it fails with 'permission denied (are you
	// root?)' This is fixed by adding CAP_NET_RAW but we don't want to do this.
	// So just use the net-tools image which doesn't have this issue for
	// whatever reason (presumably it's some difference in the ping
	// implementation) - it's enough to just set `net.ipv4.ping_group_range`.
	// (Note that podman has this same issue.)
	image := netToolsImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)
	flags.Set(t, "executor.network_stats_enabled", true)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	// Run
	cmd := &repb.Command{
		Arguments: []string{"sh", "-ec", `
			ping -c1 -W1 $(hostname)
			ping -c1 -W2 8.8.8.8
			ping -c1 -W2 example.com
		`},
	}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	t.Logf("stdout: %s", string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, 0, res.ExitCode)
	assert.GreaterOrEqual(t, res.UsageStats.GetNetworkStats().GetBytesSent(), int64(100))
	assert.GreaterOrEqual(t, res.UsageStats.GetNetworkStats().GetBytesReceived(), int64(100))
}

func TestNetwork_Disabled(t *testing.T) {
	setupNetworking(t)

	// Note: busybox has ping, but it fails with 'permission denied (are you
	// root?)' This is fixed by adding CAP_NET_RAW but we don't want to do this.
	// So just use the net-tools image which doesn't have this issue for
	// whatever reason (presumably it's some difference in the ping
	// implementation) - it's enough to just set `net.ipv4.ping_group_range`.
	// (Note that podman has this same issue.)
	image := netToolsImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)
	flags.Set(t, "executor.network_stats_enabled", true)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
		DockerNetwork:  "off",
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	// Run
	cmd := &repb.Command{
		Arguments: []string{"sh", "-ec", `
			# Should still have a loopback device available.
			ping -c1 -W1 $(hostname)

			if ping -c1 -W2 8.8.8.8 2>/dev/null; then
				echo >&2 'Should not be able to ping external network'
				exit 1
			fi
		`},
	}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	t.Logf("stdout: %s", string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, int64(0), res.UsageStats.GetNetworkStats().GetBytesSent())
	assert.Equal(t, int64(0), res.UsageStats.GetNetworkStats().GetBytesReceived())
}

func TestUser(t *testing.T) {
	setupNetworking(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)

	for _, test := range []struct {
		name       string
		props      *platform.Properties
		expectedID string
	}{
		{
			name: "Busybox/Default",
			props: &platform.Properties{
				ContainerImage: realBusyboxImage(t),
			},
			expectedID: "uid=0(root) gid=0(root) groups=0(root)",
		},
		{
			name: "Busybox/UnknownUID",
			props: &platform.Properties{
				ContainerImage: realBusyboxImage(t),
				DockerUser:     "2024",
			},
			expectedID: "uid=2024 gid=0(root) groups=0(root)",
		},
		{
			name: "Busybox/UnknownUIDAndGID",
			props: &platform.Properties{
				ContainerImage: realBusyboxImage(t),
				DockerUser:     "2024:2024",
			},
			expectedID: "uid=2024 gid=2024 groups=2024",
		},
		{
			name: "TestImage/ForceRoot",
			props: &platform.Properties{
				ContainerImage:  imageConfigTestImage(t),
				DockerForceRoot: true,
			},
			// With force-root, we only specify uid=0, and no gid.
			// So we should be running with all groups that uid 0 is a part of.
			expectedID: "uid=0(root) gid=0(root) groups=0(root),1(bin),2(daemon),3(sys),4(adm),6(disk),10(wheel),11(floppy),20(dialout),26(tape),27(video)",
		},
		{
			name: "TestImage/DefaultToImageUSER",
			props: &platform.Properties{
				ContainerImage: imageConfigTestImage(t),
			},
			// There's a USER directive specifying that buildbuddy should be
			// used, so this should override the default (root).
			expectedID: "uid=1000(buildbuddy) gid=1000(buildbuddy) groups=1000(buildbuddy)",
		},
		{
			name: "TestImage/UserProp=buildbuddy",
			props: &platform.Properties{
				ContainerImage: imageConfigTestImage(t),
				DockerUser:     "buildbuddy",
			},
			expectedID: "uid=1000(buildbuddy) gid=1000(buildbuddy) groups=1000(buildbuddy)",
		},
		{
			name: "TestImage/UserProp=1001(basil)",
			props: &platform.Properties{
				ContainerImage: imageConfigTestImage(t),
				DockerUser:     "1001",
			},
			expectedID: "uid=1001(basil) gid=1001(basil) groups=1001(basil),1002(auxgroup)",
		},
		{
			name: "TestImage/UserProp=basil:basil",
			props: &platform.Properties{
				ContainerImage: imageConfigTestImage(t),
				DockerUser:     "basil:basil",
			},
			// Extra groups aren't included when explicitly setting a group ID
			expectedID: "uid=1001(basil) gid=1001(basil) groups=1001(basil)",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			wd := testfs.MakeDirAll(t, buildRoot, uuid.New())
			c, err := provider.New(ctx, &container.Init{Props: test.props})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := c.Remove(ctx)
				require.NoError(t, err)
			})
			cmd := &repb.Command{Arguments: []string{"id"}}
			res := c.Run(ctx, cmd, wd, oci.Credentials{})
			require.NoError(t, res.Error)
			assert.Equal(t, test.expectedID, strings.TrimSpace(string(res.Stdout)))
			assert.Empty(t, string(res.Stderr))
			assert.Equal(t, 0, res.ExitCode)
		})
	}

}

func TestOverlayfsEdgeCases(t *testing.T) {
	setupNetworking(t)

	image := imageConfigTestImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	// Run
	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
		test "$(cat /test/foo)" -eq 2 || echo >&2 "/test/foo contains $(cat /test/foo) but should contain '2'"
		test -e /test/DELETED_DIR && echo >&2 "/test/DELETED_DIR unexpectedly exists"
		test -e /test/DELETED_FILE && echo >&2 "/test/DELETED_FILE unexpectedly exists"
		exit 0
	`}}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Empty(t, string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, 0, res.ExitCode)
}

func TestHighLayerCount(t *testing.T) {
	// Load busybox oci image
	busyboxImg := testregistry.ImageFromRlocationpath(t, ociBusyboxRlocationpath)

	for _, tc := range []struct {
		layerCount int
	}{
		{layerCount: 5},
		{layerCount: 20},
		{layerCount: 21},
		{layerCount: 58},
		{layerCount: 128},
	} {
		// Note that the "busybox" oci image has 1 layer
		// and the following tests will add more layers on top of it.
		t.Run(fmt.Sprintf("1And%dLayers", tc.layerCount), func(t *testing.T) {
			// Create new layers on top of busybox
			var lastContent string
			var layers []containerregistry.Layer
			for i := range tc.layerCount {
				lastContent = fmt.Sprintf("layer %d", i)
				content := []byte(lastContent)

				layer, err := crane.Layer(map[string][]byte{
					"a.txt": content,
				})
				require.NoError(t, err)

				layers = append(layers, layer)
			}
			testImg, err := mutate.AppendLayers(busyboxImg, layers...)
			require.NoError(t, err)

			// Start registry and push our new image there
			reg := testregistry.Run(t, testregistry.Opts{})
			imageRef := reg.Push(t, testImg, "foo:latest")

			// Start container to verify content
			setupNetworking(t)
			ctx := context.Background()
			env := testenv.GetTestEnv(t)
			installLeaserInEnv(t, env)
			buildRoot := testfs.MakeTempDir(t)
			cacheRoot := testfs.MakeTempDir(t)
			provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
			require.NoError(t, err)
			wd := testfs.MakeDirAll(t, buildRoot, "work")
			c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
				ContainerImage: imageRef,
			}})
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, c.Remove(ctx))
			})
			cmd := &repb.Command{Arguments: []string{"sh", "-c", `cat /a.txt`}}
			res := c.Run(ctx, cmd, wd, oci.Credentials{})
			require.NoError(t, res.Error)
			// Verify last layer wins
			assert.Equal(t, lastContent, string(res.Stdout))
			assert.Empty(t, string(res.Stderr))
			assert.Equal(t, 0, res.ExitCode)
		})
	}
}

func TestEntrypoint(t *testing.T) {
	setupNetworking(t)
	// Load busybox oci image
	busyboxImg := testregistry.ImageFromRlocationpath(t, ociBusyboxRlocationpath)
	// Mutate the image with an ENTRYPOINT directive which sets an env var
	// that we expect to be visible in the command
	cfg, err := busyboxImg.ConfigFile()
	require.NoError(t, err)
	cfg = cfg.DeepCopy()
	cfg.Config.Entrypoint = []string{"env", "FOO=bar"}
	img, err := mutate.ConfigFile(busyboxImg, cfg)
	require.NoError(t, err)
	// Start a test registry and push the mutated busybox image to it
	reg := testregistry.Run(t, testregistry.Opts{})
	image := reg.Push(t, img, "test-entrypoint:latest")
	// Set up the container
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)
	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)
	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)
	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")
	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	res := c.Run(ctx, &repb.Command{
		Arguments: []string{"sh", "-c", "echo $FOO"},
	}, wd, oci.Credentials{})

	require.NoError(t, res.Error)
	assert.Equal(t, "bar\n", string(res.Stdout))
}

func TestFileOwnership(t *testing.T) {
	setupNetworking(t)
	// Load busybox oci image
	busyboxImg := testregistry.ImageFromRlocationpath(t, ociBusyboxRlocationpath)
	// Append a layer with a file, dir, and symlink that are owned by a
	// non-root user
	layer := testregistry.NewBytesLayer(t, testtar.EntriesBytes(
		t,
		[]testtar.Entry{
			{
				Header: &tar.Header{
					Name:     "/foo.txt",
					Gid:      1000,
					Uid:      1000,
					Mode:     0644,
					Typeflag: tar.TypeReg,
				},
			},
			{
				Header: &tar.Header{
					Name:     "/bar",
					Gid:      1000,
					Uid:      1000,
					Mode:     0755,
					Typeflag: tar.TypeDir,
				},
			},
			{
				Header: &tar.Header{
					Name:     "/baz.ln",
					Gid:      1000,
					Uid:      1000,
					Mode:     0644,
					Typeflag: tar.TypeSymlink,
					Linkname: "foo.txt",
				},
			},
			{
				Header: &tar.Header{
					Name:     "/qux.hardlink",
					Typeflag: tar.TypeLink,
					Linkname: "/foo.txt",
				},
			},
		},
	))
	img, err := mutate.AppendLayers(busyboxImg, layer)
	require.NoError(t, err)
	// Start a test registry and push the mutated busybox image to it
	reg := testregistry.Run(t, testregistry.Opts{})
	image := reg.Push(t, img, "test-file-ownership:latest")
	// Set up the container
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)
	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)
	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)
	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")
	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	res := c.Run(ctx, &repb.Command{
		Arguments: []string{"stat", "-c", "%n: %u %g", "/foo.txt", "/bar", "/baz.ln", "/qux.hardlink"},
	}, wd, oci.Credentials{})

	require.NoError(t, res.Error)
	require.Empty(t, string(res.Stderr))
	assert.Equal(
		t,
		"/foo.txt: 1000 1000\n/bar: 1000 1000\n/baz.ln: 1000 1000\n/qux.hardlink: 1000 1000\n",
		string(res.Stdout),
	)
}

func TestPathSanitization(t *testing.T) {
	setupNetworking(t)
	for _, test := range []struct {
		Name          string
		Tar           []byte
		ExpectedError string
	}{
		{
			Name: "entry name",
			Tar: testtar.EntryBytes(t, &tar.Header{
				Name:     "../foo.txt",
				Uid:      os.Getuid(),
				Gid:      os.Getgid(),
				Typeflag: tar.TypeReg,
			}, nil),
			ExpectedError: "invalid tar header: name",
		},
		{
			Name: "hardlink target",
			Tar: testtar.EntryBytes(t, &tar.Header{
				Name:     "/foo.txt",
				Linkname: "../test-link-target",
				Typeflag: tar.TypeLink,
			}, nil),
			ExpectedError: "invalid tar header: link name",
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			cacheRoot := testfs.MakeTempDir(t)
			testfs.WriteAllFileContents(t, cacheRoot, map[string]string{
				"test-link-target": "Hello",
			})
			resolver, err := oci.NewResolver(te)
			require.NoError(t, err)
			require.NotNil(t, resolver)
			imageStore, err := ociruntime.NewImageStore(resolver, cacheRoot)
			require.NoError(t, err)
			// Load busybox oci image
			busyboxImg := testregistry.ImageFromRlocationpath(t, ociBusyboxRlocationpath)
			// Append an invalid layer
			layer := testregistry.NewBytesLayer(t, test.Tar)
			img, err := mutate.AppendLayers(busyboxImg, layer)
			require.NoError(t, err)
			// Start a test registry and push the mutated busybox image to it
			reg := testregistry.Run(t, testregistry.Opts{})
			image := reg.Push(t, img, "test-file-ownership:latest")

			// Make sure we get an error when pulling this image.
			ctx := context.Background()
			_, err = imageStore.Pull(ctx, image, oci.Credentials{})
			require.Error(t, err)
			assert.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got %T", err)
			assert.Contains(t, err.Error(), test.ExpectedError)
		})
	}
}

func TestPersistentWorker(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	// Create workspace with testworker binary
	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)
	ws, err := workspace.New(env, buildRoot, &workspace.Opts{Preserve: true})
	require.NoError(t, err)
	testworkerPath, err := runfiles.Rlocation(testworkerRlocationpath)
	require.NoError(t, err)
	testfs.CopyFile(t, testworkerPath, ws.Path(), "testworker")

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)

	// Create container
	require.NoError(t, err)
	err = c.Create(ctx, ws.Path())
	require.NoError(t, err)
	t.Cleanup(func() {
		err = c.Remove(ctx)
		require.NoError(t, err)
	})

	// Prepare a static response that the persistent worker will return on each
	// request.
	responseBase64 := ""
	{
		rsp := &wkpb.WorkResponse{
			Output:   "test-output",
			ExitCode: 42,
		}
		b, err := proto.Marshal(rsp)
		require.NoError(t, err)
		size := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(size, uint64(len(b)))
		responseBase64 = base64.StdEncoding.EncodeToString(append(size[:n], b...))
	}

	// Start worker (Exec)
	worker := persistentworker.Start(ctx, ws, c, "proto" /*=protocol*/, &repb.Command{
		Arguments: []string{"./testworker", "--persistent_worker", "--response_base64", responseBase64},
	})

	// Send work request.
	// The command doesn't matter - the test worker always just returns a fixed
	// response.
	res := worker.Exec(ctx, &repb.Command{})

	assert.Equal(t, "test-output", string(res.Stderr))
	assert.Equal(t, 42, res.ExitCode)

	// Pause container and stop worker
	err = c.Pause(ctx)
	require.NoError(t, err)
	err = worker.Stop()
	assert.NoError(t, err)
}

func TestCancelRun(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(context.Background())
		require.NoError(t, err)
	})

	// Run
	childID := "child" + fmt.Sprint(rand.Uint64())
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			echo "Hello world!"
			touch ./DONE
			sh -c "sleep 1000000000 # ` + childID + `" &
			sleep 1000000000
		`},
	}
	// Wait for the command to write the file "DONE" which means it is done
	// writing to stdout.
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		err := disk.WaitUntilExists(ctx, filepath.Join(wd, "DONE"), disk.WaitOpts{Timeout: -1})
		require.NoError(t, err)
	}()
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	assert.True(t, status.IsCanceledError(res.Error), "expected CanceledError, got %+#v", res.Error)
	assert.Equal(t, "Hello world!\n", string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
	// Make sure all child processes were killed.
	out := testshell.Run(t, wd, `( ps aux | grep `+childID+` | grep -v grep ) || true`)
	assert.Empty(t, out)
}

func TestCancelExec(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	// Create
	err = c.Create(ctx, wd)
	require.NoError(t, err)
	removed := false
	t.Cleanup(func() {
		if removed {
			return
		}
		err := c.Remove(context.Background())
		require.NoError(t, err)
	})
	childID := "child" + fmt.Sprint(rand.Uint64())
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			echo "Hello world!"
			touch ./DONE
			sh -c "sleep 1000000000 # ` + childID + `" &
			sleep 1000000000
		`},
	}
	// Wait for the command to write the file "DONE" which means it is done
	// writing to stdout.
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		err := disk.WaitUntilExists(ctx, filepath.Join(wd, "DONE"), disk.WaitOpts{Timeout: -1})
		require.NoError(t, err)
	}()
	res := c.Exec(ctx, cmd, &interfaces.Stdio{})
	assert.True(t, status.IsCanceledError(res.Error), "expected CanceledError, got %+#v", res.Error)
	assert.Equal(t, "Hello world!\n", string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
	// Make sure all child processes were killed.
	// In the Exec() case, it's fine if child processes stick around until we
	// call Remove().
	err = c.Remove(context.Background())
	require.NoError(t, err)
	removed = true
	out := testshell.Run(t, wd, `( ps aux | grep `+childID+` | grep -v grep ) || true`)
	assert.Empty(t, out)
}

func hasMountPermissions(t *testing.T) bool {
	dir1 := testfs.MakeTempDir(t)
	dir2 := testfs.MakeTempDir(t)
	if err := syscall.Mount(dir1, dir2, "", syscall.MS_BIND, ""); err != nil {
		return false
	}
	err := syscall.Unmount(dir2, syscall.MNT_FORCE)
	require.NoError(t, err, "unmount")
	return true
}

// TestPullImage is a simple integration test that pulls images from a public repository.
// Can be used as a smoke test to verify that the image pulling functionality works.
// This is setup as a separate, manual test target in BUILD file to help aid development.
//
// Example:
//
//	bazel test \
//	     --config=remote \
//	     --test_output=all \
//	     --test_sharding_strategy=disabled \
//	     --test_tag_filters=+docker \
//	     --test_filter=TestPullImage \
//	     --test_env=TEST_PULLIMAGE=1 \
//	     enterprise/server/remote_execution/containers/ociruntime:ociruntime_test
func TestPullImage(t *testing.T) {
	if os.Getenv("TEST_PULLIMAGE") == "" {
		t.Skip("Skipping integration test..")
	}

	for _, tc := range []struct {
		name  string
		image string
	}{
		{
			name:  "dockerhub_busybox",
			image: "busybox:latest",
		},
		{
			name:  "ghcr_nix",
			image: "ghcr.io/avdv/nix-build@sha256:5f731adacf7290352fed6c1960dfb56ec3fdb31a376d0f2170961fbc96944d50",
		},
		{
			name:  "executor_image",
			image: "gcr.io/flame-public/buildbuddy-executor-enterprise:latest",
		},
		{
			name:  "executor_docker",
			image: "gcr.io/flame-public/executor-docker-default:enterprise-v1.6.0",
		},
		{
			name:  "workflow_2004",
			image: "gcr.io/flame-public/rbe-ubuntu20-04:latest",
		},
		{
			name:  "workflow_2204",
			image: "gcr.io/flame-public/rbe-ubuntu22-04:latest",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			resolver, err := oci.NewResolver(te)
			require.NoError(t, err)
			require.NotNil(t, resolver)
			layerDir := t.TempDir()
			imgStore, err := ociruntime.NewImageStore(resolver, layerDir)
			require.NoError(t, err)

			ctx := context.Background()
			img, err := imgStore.Pull(ctx, tc.image, oci.Credentials{})
			require.NoError(t, err)
			require.NotNil(t, img)
		})
	}
}

func TestMounts(t *testing.T) {
	setupNetworking(t)
	image := manuallyProvisionedBusyboxImage(t)
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)
	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)
	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)
	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	// Configure a mount
	mountDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, mountDir, map[string]string{
		"foo.txt": "bar",
	})
	flags.Set(t, "executor.oci.mounts", []specs.Mount{
		{
			Type:        "bind",
			Source:      mountDir,
			Destination: "/mnt/testmount",
			Options:     []string{"bind", "ro"},
		},
	})

	c, err := provider.New(ctx, &container.Init{Props: &platform.Properties{
		ContainerImage: image,
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	// Run
	cmd := &repb.Command{
		Arguments: []string{"cat", "/mnt/testmount/foo.txt"},
	}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Equal(t, "bar", string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, 0, res.ExitCode)
}

func TestPersistentVolumes(t *testing.T) {
	setupNetworking(t)
	image := manuallyProvisionedBusyboxImage(t)
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	installLeaserInEnv(t, env)
	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)
	flags.Set(t, "executor.oci.enable_persistent_volumes", true)
	buildRoot := testfs.MakeTempDir(t)
	cacheRoot := testfs.MakeTempDir(t)
	provider, err := ociruntime.NewProvider(env, buildRoot, cacheRoot)
	require.NoError(t, err)
	volumes, err := platform.ParsePersistentVolumes("tmp_cache:/tmp/.cache", "user_cache:/root/.cache")
	require.NoError(t, err)
	wd1 := testfs.MakeDirAll(t, buildRoot, "work1")
	c1, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage:    image,
			PersistentVolumes: volumes,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c1.Remove(ctx)
		require.NoError(t, err)
	})
	res := c1.Run(ctx, &repb.Command{
		Arguments: []string{"touch", "/tmp/.cache/foo", "/root/.cache/bar"},
	}, wd1, oci.Credentials{})
	require.NoError(t, res.Error)
	require.Empty(t, string(res.Stderr))
	require.Equal(t, 0, res.ExitCode)

	wd2 := testfs.MakeDirAll(t, buildRoot, "work2")
	c2, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage:    image,
			PersistentVolumes: volumes,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c2.Remove(ctx)
		require.NoError(t, err)
	})
	res = c2.Run(ctx, &repb.Command{
		Arguments: []string{"stat", "/tmp/.cache/foo", "/root/.cache/bar"},
	}, wd2, oci.Credentials{})
	require.NoError(t, res.Error)
	require.Empty(t, string(res.Stderr))
	require.Equal(t, 0, res.ExitCode)
}
