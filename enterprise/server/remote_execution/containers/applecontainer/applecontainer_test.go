package applecontainer_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/applecontainer"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func writeFile(t *testing.T, parentDir, fileName, content string) {
	path := filepath.Join(parentDir, fileName)
	if err := os.WriteFile(path, []byte(content), 0660); err != nil {
		t.Fatal(err)
	}
}

func TestAppleContainerRun(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	writeFile(t, workDir, "world.txt", "world")
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
		CommandDebugString: "(applecontainer) [sh -c printf \"$GREETING $(cat world.txt)\" && printf \"foo\" >&2]",
	}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))

	provider, err := applecontainer.NewProvider(env, rootDir)
	require.NoError(t, err)

	c, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage: "docker.io/library/busybox:latest",
		},
	})
	require.NoError(t, err)

	res := c.Run(ctx, cmd, workDir, oci.Credentials{})

	assert.Equal(t, expectedResult, res)
}

func TestAppleContainerLifecycleControl(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	writeFile(t, workDir, "world.txt", "world")
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
		CommandDebugString: "(applecontainer) [sh -c printf \"$GREETING $(cat world.txt)\" && printf \"foo\" >&2]",
	}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))

	provider, err := applecontainer.NewProvider(env, rootDir)
	require.NoError(t, err)

	c, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage: "docker.io/library/busybox:latest",
		},
	})
	require.NoError(t, err)

	isContainerRunning := false
	t.Cleanup(func() {
		if isContainerRunning {
			// If we reach this line, the test failed and the container may not have
			// been removed properly. Try to remove it one last time.
			if err := c.Remove(ctx); err != nil {
				t.Log(
					"Failed to clean up apple container created by the test.",
					"You might want to remove it manually with the container CLI.",
				)
			}
		}
	})

	err = container.PullImageIfNecessary(
		ctx, env, c, oci.Credentials{},
		"docker.io/library/busybox:latest",
	)
	require.NoError(t, err)

	err = c.Create(ctx, workDir)

	require.NoError(t, err)

	// Set cleanup flag so that in case the test bails early, we can still remove
	// the container.
	isContainerRunning = true

	res := c.Exec(ctx, cmd, &interfaces.Stdio{})

	require.NoError(t, res.Error)
	assert.Equal(t, expectedResult, res)

	// Note: Pause/Unpause are not implemented for apple container
	// so we skip those tests

	stats, err := c.Stats(ctx)

	require.NoError(t, err)
	// Stats implementation is minimal, so just check it doesn't error
	assert.NotNil(t, stats)

	// Try executing the same command again
	res = c.Exec(ctx, cmd, &interfaces.Stdio{})

	require.NoError(t, res.Error)
	assert.Equal(t, expectedResult, res)

	err = c.Remove(ctx)

	require.NoError(t, err)

	// No need for cleanup anymore, since removal was successful.
	isContainerRunning = false
}

func TestAppleContainerExec_Stdio(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
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

	provider, err := applecontainer.NewProvider(env, rootDir)
	require.NoError(t, err)

	c, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage: "docker.io/library/busybox:latest",
		},
	})
	require.NoError(t, err)

	err = container.PullImageIfNecessary(
		ctx, env, c, oci.Credentials{},
		"docker.io/library/busybox:latest",
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
	assert.Equal(t, "TestOutput\n", stdout.String(), "stdout opt should be respected")
	assert.Empty(t, string(res.Stdout), "stdout in command result should be empty when stdout opt is specified")
	assert.Equal(t, "TestError\n", stderr.String(), "stderr opt should be respected")
	assert.Empty(t, string(res.Stderr), "stderr in command result should be empty when stderr opt is specified")

	err = c.Remove(ctx)
	assert.NoError(t, err)
}

func TestAppleContainerRun_LongRunningProcess_CanGetAllLogs(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
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

	provider, err := applecontainer.NewProvider(env, rootDir)
	require.NoError(t, err)

	c, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage: "docker.io/library/busybox:latest",
		},
	})
	require.NoError(t, err)

	res := c.Run(ctx, cmd, workDir, oci.Credentials{})

	assert.Equal(t, "Hello world\nHello again\n", string(res.Stdout))
}

func TestAppleContainerIsImageCached(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

	provider, err := applecontainer.NewProvider(env, rootDir)
	require.NoError(t, err)

	c, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage: "docker.io/library/busybox:latest",
		},
	})
	require.NoError(t, err)

	// First check if image is cached (might be, might not be)
	cached, err := c.IsImageCached(ctx)
	require.NoError(t, err)

	if !cached {
		// Pull the image
		err = c.PullImage(ctx, oci.Credentials{})
		require.NoError(t, err)

		// Now it should be cached
		cached, err = c.IsImageCached(ctx)
		require.NoError(t, err)
		assert.True(t, cached, "image should be cached after pulling")
	}
}

func TestAppleContainerSignal(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

	provider, err := applecontainer.NewProvider(env, rootDir)
	require.NoError(t, err)

	c, err := provider.New(ctx, &container.Init{
		Props: &platform.Properties{
			ContainerImage: "docker.io/library/busybox:latest",
		},
	})
	require.NoError(t, err)

	// Signal should fail if container is not created
	err = c.Signal(ctx, 15) // SIGTERM
	assert.Error(t, err)

	// Create the container
	err = container.PullImageIfNecessary(
		ctx, env, c, oci.Credentials{},
		"docker.io/library/busybox:latest",
	)
	require.NoError(t, err)
	err = c.Create(ctx, workDir)
	require.NoError(t, err)

	// Now signal should work
	err = c.Signal(ctx, 15) // SIGTERM
	assert.NoError(t, err)

	err = c.Remove(ctx)
	assert.NoError(t, err)
}
