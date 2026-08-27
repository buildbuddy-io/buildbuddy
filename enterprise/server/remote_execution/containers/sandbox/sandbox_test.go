//go:build darwin && !ios

package sandbox_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/sandbox"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func makeTempDirWithWorldTxt(t *testing.T) string {
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

func TestSandboxedHelloWorld(t *testing.T) {
	ctx := context.Background()
	tempDir := makeTempDirWithWorldTxt(t)
	cmd := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
		Arguments: []string{"sh", "-c", fmt.Sprintf("printf \"$GREETING $(cat %s/world.txt)!\"", tempDir)},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{
					Name:  "container-image",
					Value: "none",
				},
			},
		},
	}
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	sandboxContainer := sandbox.New(&sandbox.Options{})
	stdout, stderr, stdio := commandutil.BufferStdio()
	result := sandboxContainer.Run(ctx, cmd, tempDir, oci.Credentials{}, stdio)

	if result.Error != nil {
		t.Fatal(result.Error)
	}
	assert.Regexp(t, "^(/usr)?/bin/sandbox-exec\\s", result.CommandDebugString, "sanity check: command should be run bare")
	assert.Equal(t, "Hello world!", stdout.String(),
		"stdout should equal 'Hello world!' ('$GREETING' env var should be replaced with 'Hello', and "+
			"tempfile containing 'world' should be readable.)",
	)
	assert.Empty(t, stderr.String(), "stderr should be empty")
	// Output should only be written to the stdio writers, not buffered in the
	// command result.
	assert.Empty(t, string(result.Stdout))
	assert.Empty(t, string(result.Stderr))
	assert.Equal(t, 0, result.ExitCode, "should exit with success")
}

func TestCrossContainerReads(t *testing.T) {
	ctx := context.Background()
	tempDir1 := makeTempDirWithWorldTxt(t)
	goodCmd := &repb.Command{
		Arguments: []string{"ls", tempDir1},
	}

	// Tries to read another actions directory.
	tempDir2 := makeTempDirWithWorldTxt(t)
	evilCmd := &repb.Command{
		Arguments: []string{"ls", tempDir1},
	}

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	sandboxContainer := sandbox.New(&sandbox.Options{})
	_, goodStderr, goodStdio := commandutil.BufferStdio()
	goodResult := sandboxContainer.Run(ctx, goodCmd, tempDir1, oci.Credentials{}, goodStdio)
	_, evilStderr, evilStdio := commandutil.BufferStdio()
	evilResult := sandboxContainer.Run(ctx, evilCmd, tempDir2, oci.Credentials{}, evilStdio)

	assert.Empty(t, goodStderr.String(), "stderr should be empty")
	assert.Equal(t, 0, goodResult.ExitCode, "should exit with success")

	assert.Contains(t, evilStderr.String(), "Operation not permitted")
	assert.Equal(t, 1, evilResult.ExitCode, "should exit with error")
}
