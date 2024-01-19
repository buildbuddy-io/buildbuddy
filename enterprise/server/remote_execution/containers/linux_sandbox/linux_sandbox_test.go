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
	testFileName = "__linux_sandbox_test_file__"
)

func getBuildRoot(t *testing.T) string {
	if *buildRootDir != "" {
		err := os.MkdirAll(*buildRootDir, 0755)
		require.NoError(t, err)
		return *buildRootDir
	}
	return testfs.MakeTempDir(t)
}

func newProvider(t *testing.T, buildRoot string) container.Provider {
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
		Name            string
		Image           string
		EnableOverlayfs bool
		Command         *repb.Command

		Outputs  map[string]string
		ExitCode int
		Stdout   string
		Stderr   string
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
			Name:    "HostFS_WriteOutputFile",
			Command: newCommand("touch", "test-output"),
			Outputs: map[string]string{"test-output": ""},
		},
		{
			Name:    "ContainerFS_WriteOutputFile",
			Image:   busybox,
			Command: newCommand("touch", "test-output"),
			Outputs: map[string]string{"test-output": ""},
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
			Name: "HostFS_WritableHomeDir",
			Command: newCommand("sh", "-c", `
				mkdir -p ~/.cache/
				touch ~/.cache/foo
			`),
		},
		// {
		// 	Name:            "HostFS_CopyOnWrite",
		// 	EnableOverlayfs: true,
		// 	Command: newCommand("sh", "-c", `
		// 		for dir in /bin /usr /lib ; do
		// 			touch "${dir}/`+testFileName+`"
		// 		done
		// 	`),
		// },
		// {
		// 	Name:            "ContainerFS_CopyOnWrite",
		// 	Image:           busybox,
		// 	EnableOverlayfs: true,
		// 	Command: newCommand("sh", "-c", `
		// 		for dir in /bin /usr /lib ; do
		// 			touch "${dir}/`+testFileName+`"
		// 		done
		// 	`),
		// },
	} {
		t.Run(test.Name, func(t *testing.T) {
			if test.EnableOverlayfs && os.Getuid() != 0 {
				t.Skipf("Skipping overlayfs test - requires root but current uid is %d", os.Getuid())
			}

			ctx := context.Background()
			buildRoot := getBuildRoot(t)
			c := newContainer(ctx, t, buildRoot, test.Image)

			wd := testfs.MakeTempDir(t)
			res := c.Run(ctx, test.Command, wd, oci.Credentials{})

			require.NoError(t, res.Error)
			assert.Equal(t, test.ExitCode, res.ExitCode, "exit code")
			assert.Equal(t, test.Stdout, string(res.Stdout), "stdout")
			assert.Equal(t, test.Stderr, string(res.Stderr), "stdout")

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
		})
	}
}
