package podman_test

import (
	"bytes"
	"context"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/podman"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func writeFile(t *testing.T, parentDir, fileName, content string) {
	path := filepath.Join(parentDir, fileName)
	if err := os.WriteFile(path, []byte(content), 0660); err != nil {
		t.Fatal(err)
	}
}

func getTestEnv(t *testing.T) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	env.SetCommandRunner(&commandutil.CommandRunner{})
	return env
}

func TestRunHelloWorld(t *testing.T) {
	ctx := context.Background()
	buildRoot := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, buildRoot, "work")
	testfs.WriteAllFileContents(t, workDir, map[string]string{"world.txt": "world"})

	cmd := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			&repb.Command_EnvironmentVariable{Name: "GREETING", Value: "Hello"},
		},
		Arguments: []string{"sh", "-c", `printf "$GREETING $(cat world.txt)!"`},
	}
	// Need to give enough time to download the Docker image.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	env := getTestEnv(t)

	provider, err := podman.NewProvider(env, buildRoot)
	require.NoError(t, err)
	props := platform.Properties{
		ContainerImage: "docker.io/library/busybox",
		DockerNetwork:  "off",
	}
	c, err := provider.New(ctx, &props, nil, nil, "")
	require.NoError(t, err)
	result := c.Run(ctx, cmd, workDir, oci.Credentials{})

	require.NoError(t, result.Error)
	assert.Regexp(t, "^(/usr)?/bin/podman\\s", result.CommandDebugString, "sanity check: command should be run bare")
	assert.Equal(t, "Hello world!", string(result.Stdout),
		"stdout should equal 'Hello world!' ('$GREETING' env var should be replaced with 'Hello', and "+
			"tempfile containing 'world' should be readable.)",
	)
	assert.Empty(t, string(result.Stderr), "stderr should be empty")
	assert.Equal(t, 0, result.ExitCode, "should exit with success")
}

func TestHelloWorldExec(t *testing.T) {
	ctx := context.Background()
	buildRoot := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, buildRoot, "work")
	testfs.WriteAllFileContents(t, workDir, map[string]string{"world.txt": "world"})

	cmd := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			&repb.Command_EnvironmentVariable{Name: "GREETING", Value: "Hello"},
		},
		Arguments: []string{"sh", "-c", `printf "$GREETING $(cat world.txt)!"`},
	}
	// Need to give enough time to download the Docker image.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	env := getTestEnv(t)

	provider, err := podman.NewProvider(env, buildRoot)
	require.NoError(t, err)
	props := platform.Properties{
		ContainerImage: "docker.io/library/busybox",
		DockerNetwork:  "off",
	}
	c, err := provider.New(ctx, &props, nil, nil, "")
	require.NoError(t, err)

	err = c.Create(ctx, workDir)
	require.NoError(t, err)

	result := c.Exec(ctx, cmd, &interfaces.Stdio{})
	assert.NoError(t, result.Error)

	assert.Regexp(t, "^(/usr)?/bin/podman\\s", result.CommandDebugString, "sanity check: command should be run bare")
	assert.Equal(t, "Hello world!", string(result.Stdout),
		"stdout should equal 'Hello world!' ('$GREETING' env var should be replaced with 'Hello', and "+
			"tempfile containing 'world' should be readable.)",
	)
	assert.Empty(t, string(result.Stderr), "stderr should be empty")
	assert.Equal(t, 0, result.ExitCode, "should exit with success")

	err = c.Remove(ctx)
	assert.NoError(t, err)
}

func TestExecStdio(t *testing.T) {
	ctx := context.Background()
	buildRoot := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, buildRoot, "work")
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
	// Need to give enough time to download the Docker image.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	env := getTestEnv(t)

	provider, err := podman.NewProvider(env, buildRoot)
	require.NoError(t, err)
	props := platform.Properties{
		ContainerImage: "docker.io/library/busybox",
		DockerNetwork:  "off",
	}
	c, err := provider.New(ctx, &props, nil, nil, "")
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

func TestRun_Timeout(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
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
	env := getTestEnv(t)

	provider, err := podman.NewProvider(env, rootDir)
	require.NoError(t, err)
	props := platform.Properties{
		ContainerImage: "docker.io/library/busybox",
		DockerNetwork:  "off",
	}
	c, err := provider.New(ctx, &props, nil, nil, "")
	require.NoError(t, err)

	// Ensure the image is cached
	err = container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, "docker.io/library/busybox")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	res := c.Run(ctx, cmd, workDir, oci.Credentials{})

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

func TestExec_Timeout(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
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
	env := getTestEnv(t)

	provider, err := podman.NewProvider(env, rootDir)
	require.NoError(t, err)
	props := platform.Properties{
		ContainerImage: "docker.io/library/busybox",
		DockerNetwork:  "off",
	}
	c, err := provider.New(ctx, &props, nil, nil, "")
	require.NoError(t, err)

	// Ensure the image is cached
	err = container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, "docker.io/library/busybox")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	res := c.Run(ctx, cmd, workDir, oci.Credentials{})

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

func TestIsImageCached(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	testfs.MakeDirAll(t, rootDir, "work")
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	env := getTestEnv(t)

	tests := []struct {
		desc    string
		image   string
		want    bool
		wantErr bool
	}{
		{
			desc:    "image cached",
			image:   "docker.io/library/busybox",
			want:    true,
			wantErr: false,
		},
		{
			desc:    "image not cached",
			image:   "test.image",
			want:    false,
			wantErr: false,
		},
	}

	for _, tc := range tests {
		provider, err := podman.NewProvider(env, rootDir)
		require.NoError(t, err)
		props := platform.Properties{
			ContainerImage: tc.image,
			DockerNetwork:  "off",
		}
		c, err := provider.New(ctx, &props, nil, nil, "")
		require.NoError(t, err)
		if tc.want {
			err := c.PullImage(ctx, oci.Credentials{})
			require.NoError(t, err)
		}
		actual, err := c.IsImageCached(ctx)
		assert.Equal(t, actual, tc.want)
		if tc.wantErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestForceRoot(t *testing.T) {
	// The image used in this test doesn't have an arm64 variant yet; skip for
	// now.
	if runtime.GOARCH != "amd64" {
		t.Skipf("test is currently only supported on amd64")
	}

	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	env := getTestEnv(t)
	image := "gcr.io/flame-public/test-nonroot:test-enterprise-v1.5.4"

	cmd := &repb.Command{
		Arguments: []string{"id", "-u"},
	}

	tests := []struct {
		desc      string
		forceRoot bool
		wantUID   string
	}{
		{
			desc:      "ForceRoot=true",
			forceRoot: true,
			wantUID:   "0",
		},
		{
			desc:      "ForceRoot=false",
			forceRoot: false,
			wantUID:   "1000",
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			provider, err := podman.NewProvider(env, rootDir)
			require.NoError(t, err)
			props := platform.Properties{
				ContainerImage:  image,
				DockerForceRoot: tc.forceRoot,
				DockerNetwork:   "off",
			}
			c, err := provider.New(ctx, &props, nil, nil, "")
			require.NoError(t, err)
			result := c.Run(ctx, cmd, workDir, oci.Credentials{})
			require.NoError(t, result.Error)
			assert.Equal(t, tc.wantUID, strings.TrimSpace(string(result.Stdout)))
			assert.Empty(t, string(result.Stderr), "stderr should be empty")
			assert.Equal(t, 0, result.ExitCode, "should exit with success")
		})
	}
}

func TestUser(t *testing.T) {
	if runtime.GOARCH == "arm64" {
		// TODO: build podman ourselves, and remove this
		t.Skipf("--passwd arg is not yet supported by podman 3.4.4 (the version available on GitHub actions runner)")
	}

	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	env := getTestEnv(t)
	image := "docker.io/library/busybox"

	tests := []struct {
		name      string
		user      string
		wantUser  string
		wantGroup string
	}{
		{
			name:      "username_groupname",
			user:      "operator:operator",
			wantUser:  "operator",
			wantGroup: "operator",
		},
		{
			name:      "uid_guid",
			user:      "1000:1000",
			wantUser:  "",
			wantGroup: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider, err := podman.NewProvider(env, rootDir)
			require.NoError(t, err)
			props := platform.Properties{
				ContainerImage: image,
				DockerUser:     tc.user,
				DockerNetwork:  "off",
			}
			c, err := provider.New(ctx, &props, nil, nil, "")
			require.NoError(t, err)
			result := c.Run(ctx, &repb.Command{
				Arguments: []string{"id", "-u", "-n"},
			}, workDir, oci.Credentials{})
			u := strings.TrimSpace(string(result.Stdout))
			if tc.wantUser != "" {
				assert.Equal(t, tc.wantUser, u)
				assert.Empty(t, string(result.Stderr), "stderr should be empty")
				assert.Equal(t, 0, result.ExitCode, "should exit with success")
			} else {
				assert.Contains(t, string(result.Stderr), "unknown ID")
				assert.Equal(t, 1, result.ExitCode, "should exit with error")
			}

			result = c.Run(ctx, &repb.Command{
				Arguments: []string{"id", "-g", "-n"},
			}, workDir, oci.Credentials{})
			g := strings.TrimSpace(string(result.Stdout))
			if tc.wantGroup != "" {
				assert.Equal(t, tc.wantGroup, g)
				assert.Empty(t, string(result.Stderr), "stderr should be empty")
				assert.Equal(t, 0, result.ExitCode, "should exit with success")
			} else {
				assert.Contains(t, string(result.Stderr), "unknown ID")
				assert.Equal(t, 1, result.ExitCode, "should exit with error")
			}
		})
	}
}

func TestPodmanRun_LongRunningProcess_CanGetAllLogs(t *testing.T) {
	ctx := context.Background()
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			echo "Hello world"
			sleep 0.5
			echo "Hello again"
		`},
	}
	env := getTestEnv(t)

	provider, err := podman.NewProvider(env, rootDir)
	require.NoError(t, err)
	props := platform.Properties{
		ContainerImage: "docker.io/library/busybox",
		DockerNetwork:  "off",
	}
	c, err := provider.New(ctx, &props, nil, nil, "")
	require.NoError(t, err)

	res := c.Run(ctx, cmd, workDir, oci.Credentials{})

	assert.Equal(t, "Hello world\nHello again\n", string(res.Stdout))
}

func TestPodmanRun_RecordsStats(t *testing.T) {
	// TODO(go/b/2942): this test is fairly flaky because sometimes the cgroup
	// cpu.stat file does not contain usage_usec.
	t.Skip()

	// Note: This test requires root. Under cgroup v2, root is not required, but
	// some devs' machines are running Ubuntu 20.04 currently, which only has
	// cgroup v1 enabled (enabling cgroup v2 requires modifying kernel boot
	// params).
	u, err := user.Current()
	require.NoError(t, err)
	if u.Uid != "0" {
		t.Skip("Test requires root")
	}
	// podman needs iptables which is in /usr/sbin.
	err = os.Setenv("PATH", os.Getenv("PATH")+":/usr/sbin")
	require.NoError(t, err)

	ctx := context.Background()
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", "head -c 1000000000 /dev/urandom | sha256sum"},
	}
	env := getTestEnv(t)

	flags.Set(t, "executor.podman.enable_stats", true)
	provider, err := podman.NewProvider(env, rootDir)
	require.NoError(t, err)
	props := platform.Properties{
		ContainerImage: "docker.io/library/ubuntu:20.04",
		DockerNetwork:  "off",
	}
	c, err := provider.New(ctx, &props, nil, nil, "")
	require.NoError(t, err)

	res := c.Run(ctx, cmd, workDir, oci.Credentials{})
	require.NoError(t, res.Error)
	t.Log(string(res.Stderr))
	require.Equal(t, res.ExitCode, 0)

	require.NotNil(t, res.UsageStats, "usage stats should not be nil")
	assert.Greater(t, res.UsageStats.CpuNanos, int64(0), "CPU should be > 0")
	assert.Greater(t, res.UsageStats.PeakMemoryBytes, int64(0), "peak mem usage should be > 0")
}
