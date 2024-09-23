package ociruntime_test

import (
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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testnetworking"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	wkpb "github.com/buildbuddy-io/buildbuddy/proto/worker"
)

// Set via x_defs in BUILD file.
var crunRlocationpath string
var busyboxRlocationpath string
var testworkerRlocationpath string

func init() {
	runtimePath, err := runfiles.Rlocation(crunRlocationpath)
	if err != nil {
		log.Fatalf("Failed to locate crun in runfiles: %s", err)
	}
	*ociruntime.Runtime = runtimePath
}

func setupNetworking(t *testing.T) {
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
	return "gcr.io/flame-public/net-tools@sha256:ac701954d2c522d0d2b5296323127cacaaf77627e69db848a8d6ecb53149d344"
}

// Returns a remote reference to the image in //dockerfiles/test_images/ociruntime_test/image_config_test_image
func imageConfigTestImage(t *testing.T) string {
	if !hasMountPermissions(t) {
		t.Skipf("using a real container image with overlayfs requires mount permissions")
	}
	return "gcr.io/flame-public/image-config-test@sha256:44dc4623f3709eef89b0a6d6c8e1c3a9d54db73f6beb8cf99f402052ba9abe56"
}

func TestRun(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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

func TestCPULimit(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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
			cat /sys/fs/cgroup/cpu.max
		`},
	}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Equal(t, "max 100000\n", string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, 0, res.ExitCode)

	flags.Set(t, "executor.oci.cpu_limit", 1)
	res = c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Equal(t, "100000 100000\n", string(res.Stdout))
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, 0, res.ExitCode)
}

func TestRunUsageStats(t *testing.T) {
	setupNetworking(t)

	image := realBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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

func TestCreateExecRemove(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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

func TestExecUsageStats(t *testing.T) {
	setupNetworking(t)

	image := realBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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

func TestPullCreateExecRemove(t *testing.T) {
	setupNetworking(t)

	image := imageConfigTestImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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
	layersRoot := filepath.Join(buildRoot, "executor", "oci", "images")
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

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	// Create
	c, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage: image,
		},
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

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
	require.NoError(t, err)
	wd := testfs.MakeDirAll(t, buildRoot, "work")

	// Create
	c, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage: image,
		},
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
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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
}

func TestUser(t *testing.T) {
	setupNetworking(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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

func TestPersistentWorker(t *testing.T) {
	setupNetworking(t)

	image := manuallyProvisionedBusyboxImage(t)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	// Create workspace with testworker binary
	buildRoot := testfs.MakeTempDir(t)
	ws, err := workspace.New(env, buildRoot, &workspace.Opts{Preserve: true})
	require.NoError(t, err)
	testworkerPath, err := runfiles.Rlocation(testworkerRlocationpath)
	require.NoError(t, err)
	testfs.CopyFile(t, testworkerPath, ws.Path(), "testworker")

	provider, err := ociruntime.NewProvider(env, buildRoot)
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

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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

	runtimeRoot := testfs.MakeTempDir(t)
	flags.Set(t, "executor.oci.runtime_root", runtimeRoot)

	buildRoot := testfs.MakeTempDir(t)

	provider, err := ociruntime.NewProvider(env, buildRoot)
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
