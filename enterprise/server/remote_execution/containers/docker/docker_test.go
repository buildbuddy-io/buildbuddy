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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
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
		CommandDebugString: "(docker) [sh -c printf \"$GREETING $(cat world.txt)\" && printf \"foo\" >&2]",
	}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))
	c := docker.NewDockerContainer(env, dc, "mirror.gcr.io/library/busybox", rootDir, cfg)
	buf := &commandutil.OutputBuffers{}

	res := c.Run(ctx, cmd, buf.Stdio(), workDir, oci.Credentials{})

	assert.Equal(t, expectedResult, res)
	assert.Equal(t, "Hello world", buf.Stdout.String())
	assert.Equal(t, "foo", buf.Stderr.String())
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
		CommandDebugString: "(docker) [sh -c printf \"$GREETING $(cat world.txt)\" && printf \"foo\" >&2]",
	}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))
	c := docker.NewDockerContainer(env, dc, "mirror.gcr.io/library/busybox", rootDir, cfg)

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
		ctx, env, c, oci.Credentials{},
		"mirror.gcr.io/library/busybox",
	)
	require.NoError(t, err)

	err = c.Create(ctx, workDir)

	require.NoError(t, err)

	// Set cleanup flag so that in case the test bails early, we can still remove
	// the docker container.
	isContainerRunning = true

	buf := &commandutil.OutputBuffers{}
	res := c.Exec(ctx, cmd, buf.Stdio())

	require.NoError(t, res.Error)
	assert.Equal(t, res, expectedResult)
	assert.Equal(t, "Hello world", buf.Stdout.String())
	assert.Equal(t, "foo", buf.Stderr.String())

	err = c.Pause(ctx)

	require.NoError(t, err)

	err = c.Unpause(ctx)

	require.NoError(t, err)

	stats, err := c.Stats(ctx)

	require.NoError(t, err)
	assert.Greater(t, stats.MemoryBytes, int64(0))

	// Try executing the same command again after unpausing.
	buf = &commandutil.OutputBuffers{}
	res = c.Exec(ctx, cmd, buf.Stdio())

	require.NoError(t, res.Error)
	assert.Equal(t, res, expectedResult)
	assert.Equal(t, "Hello world", buf.Stdout.String())
	assert.Equal(t, "foo", buf.Stderr.String())

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

			# Signal to the test that we are done, then wait indefinitely
			echo "output" > output.txt
			sleep infinity
		`,
	}}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	c := docker.NewDockerContainer(env, dc, "mirror.gcr.io/library/busybox", rootDir, cfg)
	// Ensure the image is cached
	err = container.PullImageIfNecessary(
		ctx, env, c, oci.Credentials{}, "mirror.gcr.io/library/busybox")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		// Wait for output file to be created, then cancel the context.
		defer cancel()
		opts := disk.WaitOpts{Timeout: -1}
		err := disk.WaitUntilExists(ctx, filepath.Join(workDir, "output.txt"), opts)
		require.NoError(t, err)
		// Wait a little bit for stdout/stderr to be flushed to docker logs.
		time.Sleep(500 * time.Millisecond)
	}()

	buf := &commandutil.OutputBuffers{}
	res := c.Run(ctx, cmd, buf.Stdio(), workDir, oci.Credentials{})

	assert.True(
		t, status.IsUnavailableError(res.Error),
		"expected UnavailableError error, got: %s", res.Error)
	assert.Less(
		t, res.ExitCode, 0,
		"if timed out, exit code should be < 0 (unset)")
	assert.Equal(
		t, "ExampleStdout\n", buf.Stdout.String(),
		"if timed out, should be able to see debug output on stdout")
	assert.Equal(
		t, "ExampleStderr\n", buf.Stderr.String(),
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
	c := docker.NewDockerContainer(env, dc, "mirror.gcr.io/library/busybox", rootDir, cfg)
	// Ensure the image is cached
	err = container.PullImageIfNecessary(
		ctx, env, c, oci.Credentials{}, "mirror.gcr.io/library/busybox")
	require.NoError(t, err)

	err = c.Create(ctx, workDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(context.Background())
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	buf := &commandutil.OutputBuffers{}
	res := c.Exec(ctx, cmd, buf.Stdio())

	assert.True(
		t, status.IsDeadlineExceededError(res.Error),
		"expected DeadlineExceeded error, got: %s", res.Error)
	assert.Less(
		t, res.ExitCode, 0,
		"if timed out, exit code should be < 0 (unset)")
	assert.Equal(
		t, "ExampleStdout\n", buf.Stdout.String(),
		"if timed out, should be able to see debug output on stdout")
	assert.Equal(
		t, "ExampleStderr\n", buf.Stderr.String(),
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
	c := docker.NewDockerContainer(env, dc, "mirror.gcr.io/library/busybox", rootDir, cfg)
	err = container.PullImageIfNecessary(
		ctx, env, c, oci.Credentials{},
		"mirror.gcr.io/library/busybox",
	)
	require.NoError(t, err)
	err = c.Create(ctx, workDir)
	require.NoError(t, err)

	var stdout, stderr bytes.Buffer
	res := c.Exec(ctx, cmd, &interfaces.Stdio{
		Stdin:  strings.NewReader("TestInput\n"),
		Stdout: &stdout,
		Stderr: &stderr,
	})

	assert.NoError(t, res.Error)
	assert.Equal(t, "TestOutput\n", stdout.String(), "stdout")
	assert.Equal(t, "TestError\n", stderr.String(), "stderr")

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
	c := docker.NewDockerContainer(env, dc, "mirror.gcr.io/library/busybox", rootDir, cfg)

	buf := &commandutil.OutputBuffers{}
	res := c.Run(ctx, cmd, buf.Stdio(), workDir, oci.Credentials{})

	require.NoError(t, res.Error)
	assert.Equal(t, "Hello world\nHello again\n", buf.Stdout.String())
}
