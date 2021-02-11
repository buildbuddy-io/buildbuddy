package build_event_protocol_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
)

var (
	workspaceContents = map[string]string{
		"WORKSPACE": `workspace(name = "integration_test")`,
		"BUILD":     `genrule(name = "hello_txt", outs = ["hello.txt"], cmd_bash = "echo 'Hello world' > $@")`,
	}
)

func TestBuildWithBESFlags_Success(t *testing.T) {
	app := buildbuddy.Run(t)
	ctx := context.Background()
	ws := bazel.MakeTempWorkspace(t, workspaceContents)
	buildFlags := []string{"//:hello.txt"}
	buildFlags = append(buildFlags, app.BESBazelFlags()...)

	result := bazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
}
