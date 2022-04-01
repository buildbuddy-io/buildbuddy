package docker_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	dockerclient "github.com/docker/docker/client"
)

func writeFile(t *testing.T, parentDir, fileName, content string) {
	path := filepath.Join(parentDir, fileName)
	if err := ioutil.WriteFile(path, []byte(content), 0660); err != nil {
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

func TestDockerRun_ContextCanceled_StdoutStderrStillVisible(t *testing.T) {
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
			touch output
      # Wait for the context to be canceled
			sleep 100
		`,
	}}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c := docker.NewDockerContainer(env, cacheAuth, dc, "docker.io/library/busybox", rootDir, cfg)
	// Ensure the image is cached
	c.PullImage(ctx, container.PullCredentials{})

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() {
		// Cancel the context early once we know that stderr/stdout have been
		// written, to keep the test from running too long
		for !testfs.Exists(t, workDir, "output") {
			time.Sleep(5 * time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
		// Allow some time for stdout/stderr to be read from docker logs before
		// cancelling the context, since this also cancels the log read request
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	res := c.Run(ctx, cmd, workDir, container.PullCredentials{})

	assert.Equal(t, -2, res.ExitCode)
	assert.Equal(t, "ExampleStdout\n", string(res.Stdout))
	assert.Equal(t, "ExampleStderr\n", string(res.Stderr))
}

func TestDockerExec_ContextCanceled_StdoutStderrStillVisible(t *testing.T) {
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
			touch output
      # Wait for the context to be canceled
			sleep 100
		`,
	}}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c := docker.NewDockerContainer(env, cacheAuth, dc, "docker.io/library/busybox", rootDir, cfg)
	// Ensure the image is cached
	c.PullImage(ctx, container.PullCredentials{})

	err = c.Create(ctx, workDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(context.Background())
		require.NoError(t, err)
	})
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() {
		// Cancel the context early once we know that stderr/stdout have been
		// written, to keep the test from running too long
		for !testfs.Exists(t, workDir, "output") {
			time.Sleep(5 * time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
		// Allow some time for stdout/stderr to be read from docker logs before
		// cancelling the context, since this also cancels the log read request
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	res := c.Exec(ctx, cmd, nil /*=stdin*/, nil /*=stdout*/)

	assert.Equal(t, -2, res.ExitCode)
	assert.Equal(t, "ExampleStdout\n", string(res.Stdout))
	assert.Equal(t, "ExampleStderr\n", string(res.Stderr))
}
