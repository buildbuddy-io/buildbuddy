package linux_sandbox_test

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ociconv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	buildRootDir = flag.String("test_build_root_directory", "/tmp/linux_sandbox_test.buildroot", "Optional persistent build root (for unpacked container storage).")
)

const (
	busybox      = "mirror.gcr.io/library/busybox"
	testFileName = "_linux-sandbox-test-file"
)

func getBuildRoot(t *testing.T) string {
	if *buildRootDir != "" {
		err := os.MkdirAll(*buildRootDir, 0755)
		require.NoError(t, err)
		return *buildRootDir
	}
	return testfs.MakeTempDir(t)
}

func newProvider(t *testing.T, buildRoot string) *linux_sandbox.Provider {
	// Skip image auth to speed up tests - we only use public images.
	flags.Set(t, "debug_ociconv_skip_auth", true)

	// Use temp file for skopeo auth file.
	// See https://github.com/containers/skopeo/issues/1240
	tmp := testfs.MakeTempDir(t)
	err := os.Setenv("REGISTRY_AUTH_FILE", filepath.Join(tmp, "auth.json"))
	require.NoError(t, err)

	env := testenv.GetTestEnv(t)
	p, err := linux_sandbox.NewProvider(env, buildRoot)
	require.NoError(t, err)
	return p
}

func newContainer(ctx context.Context, t *testing.T, buildRoot, image string) container.CommandContainer {
	p := newProvider(t, buildRoot)
	t.Cleanup(func() {
		// Clean up sandboxd.
		p.Close()
	})
	props := &platform.Properties{
		ContainerImage: image,
	}
	c, err := p.New(ctx, props, nil, nil, "")
	require.NoError(t, err)
	return c
}

func newCommand(executable string, args ...string) *repb.Command {
	return &repb.Command{
		Arguments: append([]string{executable}, args...),
	}
}

func lines(s ...string) string {
	return strings.Join(s, "\n") + "\n"
}

func TestLinuxSandbox(t *testing.T) {
	for _, test := range []struct {
		Name string
		// Whether the test must have been run by the root user in the root user
		// namespace. i.e. it needs "true" root, not the "pseudo-root" that is
		// set up in TestMain.
		RequiresRoot bool
		Image        string
		Inputs       map[string]string
		Command      *repb.Command

		WorkspaceAfter  map[string]string
		ExitCode        int
		Stdout          string
		Stderr          string
		ExtraAssertions func()
	}{
		{
			Name:    "HostFS_NOP",
			Command: newCommand("/bin/true"),
		},
		{
			Name:    "ContainerFS_NOP",
			Image:   busybox,
			Command: newCommand("/bin/true"),
		},
		{
			Name:           "HostFS_WriteOutputFile",
			Command:        newCommand("touch", "test-output"),
			WorkspaceAfter: map[string]string{"test-output": ""},
		},
		{
			Name:    "ContainerFS_WriteOutputFile",
			Image:   busybox,
			Inputs:  map[string]string{"test-input": "foo"},
			Command: newCommand("sh", "-c", "(cat test-input && printf bar ) > ./test-output"),
			WorkspaceAfter: map[string]string{
				"test-input":  "foo",
				"test-output": "foobar",
			},
		},
		{
			// Should run as root, to match podman behavior.
			Name:    "HostFS_RunsAsRoot",
			Command: newCommand("sh", "-c", `echo "$(id -u):$(id -g)" ; echo "$HOME"`),
			Stdout:  lines("0:0", "/root"),
		},
		{
			// Should run as root, to match podman behavior.
			Name:  "ContainerFS_RunsAsRoot",
			Image: busybox,
			Command: newCommand("sh", "-c", `
				echo "$(id -u):$(id -g)"
				echo "$HOME"
			`),
			Stdout: lines("0:0", "/root"),
		},
		{
			// Test devtmpfs, procfs
			Name: "HostFS_SpecialFilesystems",
			Command: newCommand("sh", "-c", `
				cat /dev/null || exit 1
				cat /proc/self/cmdline
			`),
			Stdout: "cat\x00/proc/self/cmdline\x00",
		},
		{
			// Test devtmpfs, procfs
			Name:  "ContainerFS_SpecialFilesystems",
			Image: busybox,
			Command: newCommand("sh", "-c", `
				cat /dev/null || exit 1
				cat /proc/self/cmdline
			`),
			Stdout: "cat\x00/proc/self/cmdline\x00",
		},
		{
			Name:         "HostFS_CopyOnWrite",
			RequiresRoot: true,
			// Write "/{bin,usr,lib}/$testFileName" in the sandbox. The test
			// body will verify that these files do not exist outside the
			// sandbox afterwards.
			Command: newCommand("sh", "-c", `
				for dir in /bin /usr /lib ; do
					touch "${dir}/`+testFileName+`"
				done
			`),
		},
		{
			Name: "ContainerFS_CopyOnWrite",
			// Note: we do not have a HostFS_CopyOnWrite test because it would
			// require /usr, /bin etc. to be writable by the test user, which
			// would require running bazel as root (sort of gross)
			Image: busybox,
			// Write "/{bin,usr,lib}/$testFileName" in the sandbox. The test
			// body will verify that these files do not exist outside the
			// sandbox afterwards.
			Command: newCommand("sh", "-c", `
				for dir in /bin /usr /lib ; do
					touch "${dir}/`+testFileName+`"
				done
			`),
		},
		{
			Name:  "ContainerFS_Remove_KillsBackgroundProcesses",
			Image: busybox,
			// Run a command that spawns a BG process which tries to keep file
			// handles open in the overlayfs. These processes should be killed
			// and we should be able to successfully unmount the overlayfs.
			Command: newCommand("sh", "-c", `
				sh -c '
					cd /bin
					(
						printf "_LINUX_SANDBOX_BG_PROCESS"
						while true; do
							sleep 1
						done
					) >> `+testFileName+`
				' &
				while ! [ -e /bin/`+testFileName+` ]; do
					sleep 0.01
				done
			`),
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			if test.RequiresRoot {
				if os.Getuid() != 0 {
					t.Skipf("test requires root")
				}
			}

			ctx := context.Background()
			buildRoot := getBuildRoot(t)
			c := newContainer(ctx, t, buildRoot, test.Image)
			t.Cleanup(func() {
				err := c.Remove(ctx)
				assert.NoError(t, err, "remove")
			})

			wd := testfs.MakeTempDir(t)
			testfs.WriteAllFileContents(t, wd, test.Inputs)
			res := c.Run(ctx, test.Command, wd, oci.Credentials{})

			require.NoError(t, res.Error)
			assert.Equal(t, test.ExitCode, res.ExitCode, "exit code")
			assert.Equal(t, test.Stdout, string(res.Stdout), "stdout")
			assert.Equal(t, test.Stderr, string(res.Stderr), "stderr")

			testfs.AssertExactFileContents(t, wd, test.WorkspaceAfter)

			// Make sure the host FS was not modified.
			// NOTE: if this fails, the test may become permanently borked
			// and the files need to be manually removed.
			dirsToCheck := []string{"/bin", "/usr", "/lib"}
			for _, d := range dirsToCheck {
				exists := testfs.Exists(t, filepath.Join(d, testFileName))
				assert.False(t, exists, "%s should not exist after execution", filepath.Join(d, testFileName))
				_ = os.RemoveAll(filepath.Join(d, testFileName))
			}

			// If applicable, make sure the base image was not modified.
			if test.Image != "" {
				rootFSPath, err := ociconv.CachedRootFSPath(ctx, buildRoot, test.Image)
				require.NoError(t, err, "image rootfs path")
				for _, d := range dirsToCheck {
					exists := testfs.Exists(t, filepath.Join(rootFSPath, d, testFileName))
					assert.False(t, exists, "%s should not exist after execution", filepath.Join(d, testFileName))
					_ = os.RemoveAll(filepath.Join(rootFSPath, d, testFileName))
				}
			}

			if test.ExtraAssertions != nil {
				test.ExtraAssertions()
			}
		})
	}
}
