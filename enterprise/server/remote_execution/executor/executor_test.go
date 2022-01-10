package executor

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"

	gstatus "google.golang.org/grpc/status"
)

func getTestExecutor(t *testing.T) *Executor {
	env := testenv.GetTestEnv(t)
	id := "test-executor-id"
	ex, err := NewExecutor(env, id, &Options{})
	if err != nil {
		t.Fatal(err)
	}
	return ex
}

func makeTempDirWithWorldTxt(t *testing.T) string {
	rootDirFlag := flag.Lookup("executor.root_directory")
	if rootDirFlag == nil {
		t.Fatal("Missing --executor.root_directory flag.")
	}
	dir := testfs.MakeTempDir(t)

	f, err := os.Create(fmt.Sprintf("%s/world.txt", dir))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, err = f.WriteString("world")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestHelloWorldOnBareMetal(t *testing.T) {
	ex := getTestExecutor(t)
	ctx := context.Background()
	tempDir := makeTempDirWithWorldTxt(t)
	cmd := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			&repb.Command_EnvironmentVariable{Name: "GREETING", Value: "Hello"},
		},
		Arguments: []string{"sh", "-c", fmt.Sprintf("printf \"$GREETING $(cat %s/world.txt)!\"", tempDir)},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				&repb.Platform_Property{
					Name:  "container-image",
					Value: "none",
				},
			},
		},
	}
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	result := ex.executeCommand(ctx, cmd, tempDir)

	if result.Error != nil {
		t.Fatal(result.Error)
	}
	assert.Regexp(t, "^(/usr)?/bin/sh\\s", result.CommandDebugString, "sanity check: command should be run bare")
	assert.Equal(t, "Hello world!", string(result.Stdout),
		"stdout should equal 'Hello world!' ('$GREETING' env var should be replaced with 'Hello', and "+
			"tempfile containing 'world' should be readable.)",
	)
	assert.Empty(t, string(result.Stderr), "stderr should be empty")
	assert.Equal(t, 0, result.ExitCode, "should exit with success")
}

func TestHelloWorldOnDocker(t *testing.T) {
	flags.Set(t, "executor.docker_socket", "/var/run/docker.sock")
	ex := getTestExecutor(t)
	ctx := context.Background()
	tempDir := makeTempDirWithWorldTxt(t)
	cmd := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			&repb.Command_EnvironmentVariable{Name: "GREETING", Value: "Hello"},
		},
		Arguments: []string{"sh", "-c", `printf "$GREETING $(cat world.txt)!"`},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				&repb.Platform_Property{
					Name:  "container-image",
					Value: "docker://gcr.io/flame-public/executor-docker-default:enterprise-v1.4.51",
				},
			},
		},
	}
	// Need to give enough time to download the Docker image.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	result := ex.executeCommand(ctx, cmd, tempDir)

	if result.Error != nil {
		t.Fatal(result.Error)
	}
	assert.Regexp(t, "^\\(docker\\)", result.CommandDebugString, "sanity check: command should be run with docker")
	assert.Equal(t, "Hello world!", string(result.Stdout),
		"stdout should equal 'Hello world!' ('$GREETING' env var should be replaced with 'Hello', and "+
			"tempfile containing 'world' should be readable.)",
	)
	assert.Empty(t, string(result.Stderr), "stderr should be empty")
	assert.Equal(t, 0, result.ExitCode, "should exit with success")
}

func TestTimeoutOnDocker(t *testing.T) {
	flags.Set(t, "executor.docker_socket", "/var/run/docker.sock")

	ex := getTestExecutor(t)
	ctx := context.Background()
	tempDir := makeTempDirWithWorldTxt(t)
	// Execute a no-op command once to allow time for the image to be
	// pulled if it's not cached already.
	initCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	result := ex.executeCommand(initCtx, containerizedShCommand(": NOP"), tempDir)
	if result.Error != nil {
		t.Fatal(result.Error)
	}
	ctx, cancel = context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	result = ex.executeCommand(ctx, containerizedShCommand("/bin/sleep 10"), tempDir)

	if result.Error != nil && gstatus.Code(result.Error) != codes.DeadlineExceeded {
		t.Fatal(result.Error)
	}
	assert.Equal(t, codes.DeadlineExceeded.String(), gstatus.Code(result.Error).String())
}

func containerizedShCommand(cmd string) *repb.Command {
	return &repb.Command{
		Arguments: []string{"/bin/sh", "-c", cmd},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				&repb.Platform_Property{
					Name:  "container-image",
					Value: "docker://gcr.io/flame-public/executor-docker-default:enterprise-v1.4.51",
				},
			},
		},
	}
}

func TestNoRunAsRootOnDocker(t *testing.T) {
	flags.Set(t, "executor.docker_socket", "/var/run/docker.sock")
	ex := getTestExecutor(t)
	ctx := context.Background()
	tempDir := makeTempDirWithWorldTxt(t)
	cmd := &repb.Command{
		Arguments: []string{"id", "-u"},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				&repb.Platform_Property{
					Name:  "container-image",
					Value: "docker://gcr.io/flame-build/test-nonroot-user:test_image",
				},
			},
		},
	}
	// Need to give enough time to download the Docker image.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	result := ex.executeCommand(ctx, cmd, tempDir)

	if result.Error != nil {
		t.Fatal(result.Error)
	}
	assert.Regexp(t, "^\\(docker\\)", result.CommandDebugString, "sanity check: command should be run with docker")
	assert.Equal(t, "1000\n", string(result.Stdout), "stdout should equal '1000' (the uid of the default user) with a terminating new line")
	assert.Empty(t, string(result.Stderr), "stderr should be empty")
	assert.Equal(t, 0, result.ExitCode, "should exit with success")
}

func TestRunAsRootOnDocker(t *testing.T) {
	flags.Set(t, "executor.docker_socket", "/var/run/docker.sock")
	ex := getTestExecutor(t)
	ctx := context.Background()
	tempDir := makeTempDirWithWorldTxt(t)
	cmd := &repb.Command{
		Arguments: []string{"id", "-u"},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				&repb.Platform_Property{
					Name:  "container-image",
					Value: "docker://gcr.io/flame-build/test-nonroot-user:test_image",
				},
				&repb.Platform_Property{
					Name:  "dockerRunAsRoot",
					Value: "true",
				},
			},
		},
	}
	// Need to give enough time to download the Docker image.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	result := ex.executeCommand(ctx, cmd, tempDir)

	if result.Error != nil {
		t.Fatal(result.Error)
	}
	assert.Regexp(t, "^\\(docker\\)", result.CommandDebugString, "sanity check: command should be run with docker")
	assert.Equal(t, "0\n", string(result.Stdout), "stdout should equal '0' (the uid of the root user) with a terminating new line")
	assert.Empty(t, string(result.Stderr), "stderr should be empty")
	assert.Equal(t, 0, result.ExitCode, "should exit with success")
}
