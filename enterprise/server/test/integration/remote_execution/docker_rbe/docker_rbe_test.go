package docker_rbe_test

import (
	"context"
	"os"
	"runtime"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestDockerWithOverlayfs_InputFilesAreImmutable(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skipf("Test requires root for overlay mount() syscall")
	}
	hasDockerd, err := disk.FileExists(context.TODO(), "/var/run/docker.sock")
	require.NoError(t, err)
	if !hasDockerd {
		t.Skipf("Test requires docker socket at /var/run/docker.sock")
	}

	flags.Set(t, "executor.docker_socket", "/var/run/docker.sock")
	flags.Set(t, "executor.workspace.overlayfs_enabled", true)

	tmpDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, tmpDir, map[string]string{
		"input.txt": "input_txt_content",
	})

	rbe := rbetest.NewRBETestEnv(t)
	rbe.AddBuildBuddyServer()
	rbe.AddExecutor(t)

	platform := &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: runtime.GOOS},
			{Name: "Arch", Value: runtime.GOARCH},
			{Name: "container-image", Value: "docker://busybox"},
		},
	}

	for i := 1; i <= 2; i++ {
		opts := &rbetest.ExecuteOpts{InputRootDir: tmpDir}
		cmd := rbe.Execute(&repb.Command{
			Arguments: []string{"sh", "-c", `
				# Make sure the input has the expected contents.
				if test "$(cat input.txt)" != "input_txt_content" ; then
					printf >&2 'input.txt contains unexpected contents %s\n' "$(cat input.txt)"
					exit 1
				fi

				# Create the expected output file.
				printf 'output_txt_content' > output.txt

				# Try to mess up the input file for the next action; this
				# should not work.
				printf '-MODIFIED' >> input.txt
			`},
			Platform:    platform,
			OutputFiles: []string{"output.txt"},
		}, opts)
		res := cmd.Wait()

		require.Equal(t, 0, res.ExitCode, "stderr: %s", res.Stderr)
		outDir := rbe.DownloadOutputsToNewTempDir(res)
		testfs.AssertExactFileContents(t, outDir, map[string]string{
			"output.txt": "output_txt_content",
		})
	}
}
