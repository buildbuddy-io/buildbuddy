package bare_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare"
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

func TestHelloWorldOnBareMetal(t *testing.T) {
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

	bareContainer := bare.NewBareCommandContainer(&bare.Opts{})
	buf := &commandutil.OutputBuffers{}
	result := bareContainer.Run(ctx, cmd, buf.Stdio(), tempDir, oci.Credentials{})

	if result.Error != nil {
		t.Fatal(result.Error)
	}
	assert.Regexp(t, "^(/usr)?/bin/sh\\s", result.CommandDebugString, "sanity check: command should be run bare")
	assert.Equal(t, "Hello world!", buf.Stdout.String(),
		"stdout should equal 'Hello world!' ('$GREETING' env var should be replaced with 'Hello', and "+
			"tempfile containing 'world' should be readable.)",
	)
	assert.Empty(t, buf.Stderr.String(), "stderr should be empty")
	assert.Equal(t, 0, result.ExitCode, "should exit with success")
}
