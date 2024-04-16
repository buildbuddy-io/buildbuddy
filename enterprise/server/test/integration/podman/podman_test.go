package podman_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
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

var (
	// rlocationpath for podman-static.tar.gz.
	// Populated by x_defs in BUILD file.
	podmanArchiveRlocationpath string
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

func installPodman() error {
	// TODO: make this work even when not running inside a VM. We should be able
	// to run podman-static directly from the runfiles directory and configure
	// podman to only use the tools/configs from this directory rather than the
	// system directories.

	// Install the podman version at HEAD by extracting the podman-static
	// distribution under /.
	existenceFile := "/.podman_test.podman_installed"
	if _, err := os.Stat(existenceFile); err == nil {
		return nil // Podman is already installed
	} else if !os.IsNotExist(err) {
		return err
	}
	// We haven't installed podman. First check that the execution image we're
	// using doesn't have podman installed already, otherwise it may conflict
	// with the one we're trying to install.
	if path, err := exec.LookPath("podman"); err == nil {
		return fmt.Errorf("install podman: %s already installed in runner", path)
	}

	podmanArchiveAbspath, err := runfiles.Rlocation(podmanArchiveRlocationpath)
	if err != nil {
		return fmt.Errorf("locate podman in runfiles: %w", err)
	}
	cmd := exec.Command("tar", "--extract", "--file", podmanArchiveAbspath, "--directory=/")
	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("extract podman-static: %w (output: %q)", err, string(b))
	}
	if err := os.WriteFile(existenceFile, nil, 0644); err != nil {
		return fmt.Errorf("create %s: %w", existenceFile, err)
	}
	return nil
}

func TestMain(m *testing.M) {
	// When running on arm64 github runners, execute the test using sudo.
	// This is for two reasons:
	// - Tests on amd64 (firecracker) run as root, and ideally we'd run as
	//   root on both amd64 and arm64 for consistency.
	// - We need root in order to install podman-static under /.
	if runtime.GOARCH == "arm64" && os.Getuid() != 0 {
		args := append([]string{"sudo", "--non-interactive", "--preserve-env"}, os.Args...)
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			if cmd.Process != nil && cmd.ProcessState.Exited() {
				os.Exit(cmd.ProcessState.ExitCode())
			}
			log.Fatal(err)
		}
		os.Exit(0)
	}

	// Install podman-static in the test VM if it isn't installed already.
	if err := installPodman(); err != nil {
		log.Fatalf("Failed to install podman: %s", err)
	}
	// Prevent podman from reading ~/.docker/config.json which causes the gcr
	// credential helper to be used, which causes authentication to fail when
	// pulling our custom test images.
	if err := os.WriteFile("/tmp/auth.json", []byte("{}"), 0644); err != nil {
		log.Fatalf("Failed to write registry auth config: %s", err)
	}
	os.Setenv("REGISTRY_AUTH_FILE", "/tmp/auth.json")
	os.Exit(m.Run())
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
