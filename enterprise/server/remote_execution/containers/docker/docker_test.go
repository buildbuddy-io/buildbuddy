package docker_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	dockerclient "github.com/docker/docker/client"
)

func writeFile(t *testing.T, parentDir, fileName, content string) {
	path := filepath.Join(parentDir, fileName)
	if err := os.WriteFile(path, []byte(content), 0660); err != nil {
		t.Fatal(err)
	}
}

func TestDockerRun(t *testing.T) {
	socket := "/var/run/docker.sock"
	dc, err := dockerclient.NewClientWithOpts(
		dockerclient.WithHost(fmt.Sprintf("unix://%s", socket)),
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	writeFile(t, workDir, "world.txt", "world")
	cfg := &docker.DockerOptions{Socket: socket, InheritUserIDs: true}
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
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c := docker.NewDockerContainer(env, cacheAuth, dc, "docker.io/library/busybox", rootDir, cfg)

	res := c.Run(ctx, cmd, workDir, container.PullCredentials{})

	assert.Equal(t, expectedResult, res)
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
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	writeFile(t, workDir, "world.txt", "world")
	cfg := &docker.DockerOptions{Socket: socket, InheritUserIDs: true}
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
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c := docker.NewDockerContainer(env, cacheAuth, dc, "docker.io/library/busybox", rootDir, cfg)

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

	err = container.PullImageIfNecessary(
		ctx, env, cacheAuth, c, container.PullCredentials{},
		"docker.io/library/busybox",
	)
	require.NoError(t, err)

	err = c.Create(ctx, workDir)

	require.NoError(t, err)

	// Set cleanup flag so that in case the test bails early, we can still remove
	// the docker container.
	isContainerRunning = true

	res := c.Exec(ctx, cmd, &container.Stdio{})

	require.NoError(t, res.Error)
	assert.Equal(t, res, expectedResult)

	err = c.Pause(ctx)

	require.NoError(t, err)

	err = c.Unpause(ctx)

	require.NoError(t, err)

	stats, err := c.Stats(ctx)

	require.NoError(t, err)
	assert.Greater(t, stats.MemoryBytes, int64(0))

	// Try executing the same command again after unpausing.
	res = c.Exec(ctx, cmd, &container.Stdio{})

	require.NoError(t, res.Error)
	assert.Equal(t, res, expectedResult)

	err = c.Remove(ctx)

	require.NoError(t, err)

	// No need for cleanup anymore, since removal was successful.
	isContainerRunning = false
}

func TestDockerRun_Timeout_StdoutStderrStillVisible(t *testing.T) {
	socket := "/var/run/docker.sock"
	dc, err := dockerclient.NewClientWithOpts(
		dockerclient.WithHost(fmt.Sprintf("unix://%s", socket)),
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	writeFile(t, workDir, "world.txt", "world")
	cfg := &docker.DockerOptions{Socket: socket, InheritUserIDs: true}
	ctx := context.Background()
	cmd := &repb.Command{Arguments: []string{
		"sh", "-c", `
			echo ExampleStdout >&1
			echo ExampleStderr >&2
			echo "output" > output.txt
      # Wait for the context to be canceled
			sleep 100
		`,
	}}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c := docker.NewDockerContainer(env, cacheAuth, dc, "docker.io/library/busybox", rootDir, cfg)
	// Ensure the image is cached
	err = container.PullImageIfNecessary(
		ctx, env, cacheAuth, c, container.PullCredentials{}, "docker.io/library/busybox")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	res := c.Run(ctx, cmd, workDir, container.PullCredentials{})

	assert.True(
		t, status.IsDeadlineExceededError(res.Error),
		"expected DeadlineExceeded error, got: %s", res.Error)
	assert.Less(
		t, res.ExitCode, 0,
		"if timed out, exit code should be < 0 (unset)")
	assert.Equal(
		t, "ExampleStdout\n", string(res.Stdout),
		"if timed out, should be able to see debug output on stdout")
	assert.Equal(
		t, "ExampleStderr\n", string(res.Stderr),
		"if timed out, should be able to see debug output on stderr")
	output := testfs.ReadFileAsString(t, workDir, "output.txt")
	assert.Equal(
		t, "output\n", output,
		"if timed out, should be able to read debug output files")
}

func TestDockerExec_Timeout_StdoutStderrStillVisible(t *testing.T) {
	socket := "/var/run/docker.sock"
	dc, err := dockerclient.NewClientWithOpts(
		dockerclient.WithHost(fmt.Sprintf("unix://%s", socket)),
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	cfg := &docker.DockerOptions{Socket: socket, InheritUserIDs: true}
	ctx := context.Background()
	cmd := &repb.Command{Arguments: []string{
		"sh", "-c", `
			echo ExampleStdout >&1
			echo ExampleStderr >&2
			echo "output" > output.txt
      # Wait for the context to be canceled
			sleep 100
		`,
	}}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c := docker.NewDockerContainer(env, cacheAuth, dc, "docker.io/library/busybox", rootDir, cfg)
	// Ensure the image is cached
	err = container.PullImageIfNecessary(
		ctx, env, cacheAuth, c, container.PullCredentials{}, "docker.io/library/busybox")
	require.NoError(t, err)

	err = c.Create(ctx, workDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(context.Background())
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	res := c.Exec(ctx, cmd, &container.Stdio{})

	assert.True(
		t, status.IsDeadlineExceededError(res.Error),
		"expected DeadlineExceeded error, got: %s", res.Error)
	assert.Less(
		t, res.ExitCode, 0,
		"if timed out, exit code should be < 0 (unset)")
	assert.Equal(
		t, "ExampleStdout\n", string(res.Stdout),
		"if timed out, should be able to see debug output on stdout")
	assert.Equal(
		t, "ExampleStderr\n", string(res.Stderr),
		"if timed out, should be able to see debug output on stderr")
	output := testfs.ReadFileAsString(t, workDir, "output.txt")
	assert.Equal(
		t, "output\n", output,
		"if timed out, should be able to read debug output files")
}

func TestDockerExec_Stdio(t *testing.T) {
	socket := "/var/run/docker.sock"
	dc, err := dockerclient.NewClientWithOpts(
		dockerclient.WithHost(fmt.Sprintf("unix://%s", socket)),
		dockerclient.WithAPIVersionNegotiation(),
	)
	require.NoError(t, err)
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	cfg := &docker.DockerOptions{Socket: socket, InheritUserIDs: true}
	ctx := context.Background()
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			if ! [ $(cat) = "TestInput" ]; then
				echo "ERROR: missing expected TestInput on stdin"
				exit 1
			fi

			echo TestOutput
			echo TestError >&2
		`},
	}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c := docker.NewDockerContainer(env, cacheAuth, dc, "docker.io/library/busybox", rootDir, cfg)
	err = container.PullImageIfNecessary(
		ctx, env, cacheAuth, c, container.PullCredentials{},
		"docker.io/library/busybox",
	)
	require.NoError(t, err)
	err = c.Create(ctx, workDir)
	require.NoError(t, err)

	var stdout, stderr bytes.Buffer
	res := c.Exec(ctx, cmd, &container.Stdio{
		Stdin:  strings.NewReader("TestInput\n"),
		Stdout: &stdout,
		Stderr: &stderr,
	})

	assert.NoError(t, res.Error)
	assert.Equal(t, "TestOutput\n", stdout.String(), "stdout opt should be respected")
	assert.Empty(t, string(res.Stdout), "stdout in command result should be empty when stdout opt is specified")
	assert.Equal(t, "TestError\n", stderr.String(), "stderr opt should be respected")
	assert.Empty(t, string(res.Stderr), "stderr in command result should be empty when stderr opt is specified")

	err = c.Remove(ctx)
	assert.NoError(t, err)
}

func TestDockerRun_LongRunningProcess_CanGetAllLogs(t *testing.T) {
	socket := "/var/run/docker.sock"
	dc, err := dockerclient.NewClientWithOpts(
		dockerclient.WithHost(fmt.Sprintf("unix://%s", socket)),
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	cfg := &docker.DockerOptions{Socket: socket, InheritUserIDs: true}
	ctx := context.Background()
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			echo "Hello world"
			sleep 0.5
			echo "Hello again"
		`},
	}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c := docker.NewDockerContainer(env, cacheAuth, dc, "docker.io/library/busybox", rootDir, cfg)

	res := c.Run(ctx, cmd, workDir, container.PullCredentials{})

	assert.Equal(t, "Hello world\nHello again\n", string(res.Stdout))
}
