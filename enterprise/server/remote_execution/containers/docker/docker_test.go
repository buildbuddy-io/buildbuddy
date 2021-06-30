package docker_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	dockerclient "github.com/docker/docker/client"
)

func makeWorkDir(t testing.TB, rootDir string) string {
	workDir, err := ioutil.TempDir(rootDir, "ws_*")
	if err != nil {
		t.Fatal(err)
	}
	return workDir
}

func writeFile(t *testing.T, parentDir, fileName, content string) {
	path := filepath.Join(parentDir, fileName)
	if err := ioutil.WriteFile(path, []byte(content), 0660); err != nil {
		t.Fatal(err)
	}
}

func TestDockerLifecycleControl(t *testing.T) {
	socket := "/var/run/docker.sock"
	dc, err := dockerclient.NewClientWithOpts(
		dockerclient.WithHost(fmt.Sprintf("unix://%s", socket)),
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	rootDir := testfs.MakeTempDir(t)
	workDir := makeWorkDir(t, rootDir)
	writeFile(t, workDir, "world.txt", "world")
	cfg := &docker.DockerOptions{Socket: socket}
	ctx := context.Background()
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `printf "$GREETING $(cat world.txt)" && printf "foo" >&2`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	expectedResult := &interfaces.CommandResult{
		ExitCode:           0,
		Stdout:             []byte("Hello world"),
		Stderr:             []byte("foo"),
		CommandDebugString: "(docker) [sh -c printf \"$GREETING $(cat world.txt)\" && printf \"foo\" >&2]",
	}
	c := docker.NewDockerContainer(dc, "docker.io/library/busybox", rootDir, cfg)

	isContainerRunning := false
	t.Cleanup(func() {
		if isContainerRunning {
			// If we reach this line, the test failed and the container may not have
			// been removed properly. Try to remove it one last time.
			if err := c.Remove(ctx); err != nil {
				t.Log(
					"Failed to clean up docker container created by the test.",
					"You might want to remove it manually with the docker CLI.",
					"(Look for containers named 'executor_*')",
				)
			}
		}
	})

	err = c.Create(ctx, workDir)

	require.NoError(t, err)

	// Set cleanup flag so that in case the test bails early, we can still remove
	// the docker container.
	isContainerRunning = true

	res := c.Exec(ctx, cmd, nil, nil)

	require.NoError(t, res.Error)
	assert.Equal(t, res, expectedResult)

	err = c.Pause(ctx)

	require.NoError(t, err)

	err = c.Unpause(ctx)

	require.NoError(t, err)

	stats, err := c.Stats(ctx)

	require.NoError(t, err)
	assert.Greater(t, stats.MemoryUsageBytes, int64(0))

	// Try executing the same command again after unpausing.
	res = c.Exec(ctx, cmd, nil, nil)

	require.NoError(t, res.Error)
	assert.Equal(t, res, expectedResult)

	err = c.Remove(ctx)

	require.NoError(t, err)

	// No need for cleanup anymore, since removal was successful.
	isContainerRunning = false
}
