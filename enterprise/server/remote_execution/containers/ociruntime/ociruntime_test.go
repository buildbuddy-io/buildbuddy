package ociruntime_test

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// Set via x_defs in BUILD file.
var crunRlocationpath string

func init() {
	// Set up cgroup v2-only on Firecracker.
	// TODO: remove once guest cgroup v2-only mode is enabled by default.
	b, err := exec.Command("sh", "-ex", "-c", `
		[ -e  /sys/fs/cgroup/unified ] || exit 0
		umount -f /sys/fs/cgroup/unified
		rmdir /sys/fs/cgroup/unified
		mount | grep /sys/fs/cgroup/ | awk '{print $3}' | xargs umount -f
		umount -f /sys/fs/cgroup
		mount cgroup2 /sys/fs/cgroup -t cgroup2 -o "nsdelegate"
	`).CombinedOutput()
	if err != nil {
		log.Fatalf("Failed to set up cgroup2: %s: %q", err, strings.TrimSpace(string(b)))
	}

	runtimePath, err := runfiles.Rlocation(crunRlocationpath)
	if err != nil {
		log.Fatalf("Failed to locate crun in runfiles: %s", err)
	}
	*ociruntime.Runtime = runtimePath
}

// Returns a special image ref indicating that a busybox-based rootfs should
// be provisioned using the 'busybox' binary on the host machine.
// Skips the test if busybox is not available.
// TODO: use a static test binary + empty rootfs, instead of relying on busybox
// for testing
func manuallyProvisionedBusyboxImage(t *testing.T) string {
	if _, err := exec.LookPath("busybox"); err != nil {
		t.Skipf("skipping test due to missing busybox: %s", err)
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

func TestRun(t *testing.T) {
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
			echo "$GREETING world!"
		`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Equal(t, "Hello world!\n", string(res.Stdout))
	assert.Empty(t, "", string(res.Stderr))
	assert.Equal(t, 0, res.ExitCode)
}

func TestRunUsageStats(t *testing.T) {
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
	assert.Greater(t, res.UsageStats.GetMemoryBytes(), int64(0), "memory")
	assert.Greater(t, res.UsageStats.GetCpuNanos(), int64(0), "CPU")
}

func TestRunWithImage(t *testing.T) {
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

	// Run
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			echo "$GREETING world!"
		`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	res := c.Run(ctx, cmd, wd, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Equal(t, "Hello world!\n", string(res.Stdout))
	assert.Empty(t, "", string(res.Stderr))
	assert.Equal(t, 0, res.ExitCode)
}

func TestCreateExecRemove(t *testing.T) {
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
	image := realBusyboxImage(t)

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
	cmd := &repb.Command{Arguments: []string{"sh", "-ec", `
		touch /bin/foo.txt
		pwd
	`}}
	stdio := interfaces.Stdio{}
	res := c.Exec(ctx, cmd, &stdio)
	require.NoError(t, res.Error)

	assert.Equal(t, 0, res.ExitCode)
	assert.Empty(t, string(res.Stderr))
	assert.Equal(t, "/buildbuddy-execroot\n", string(res.Stdout))

	// Make sure the image layers were unmodified and that foo.txt was written
	// to the upper dir in the overlayfs.
	layersRoot := filepath.Join(buildRoot, "executor", "oci", "layers")
	err = filepath.WalkDir(layersRoot, func(path string, entry fs.DirEntry, err error) error {
		require.NoError(t, err)
		assert.NotEqual(t, entry.Name(), "foo.txt")
		return nil
	})
	require.NoError(t, err)
	assert.True(t, testfs.Exists(t, "", filepath.Join(wd+".overlay", "upper", "bin", "foo.txt")))
}

func TestCreateExecPauseUnpause(t *testing.T) {
	if runtime.GOARCH == "arm64" {
		// TODO: fix test on arm64. It fails due to some issue with cgroups on
		// the GH Actions runners.
		t.Skipf("test is skipped on arm64")
	}

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
	require.Equal(t, d1, d2, "expected date file not to be updated")

	// Unpause
	err = c.Unpause(ctx)
	require.NoError(t, err)

	// Counter should be getting continually updated again since we're unpaused.
	waitUntilCounterIncremented()
}

func TestCreateFailureHasStderr(t *testing.T) {
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
