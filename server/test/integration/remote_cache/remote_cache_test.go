package remote_cache_test

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

func TestBazelBuild_RemoteCacheHit(t *testing.T) {
	app := buildbuddy.Run(t)
	ctx := context.Background()
	ws := bazel.MakeTempWorkspace(t, workspaceContents)
	buildFlags := []string{"//:hello.txt", "--remote_upload_local_results"}
	buildFlags = append(buildFlags, app.BESBazelFlags()...)
	buildFlags = append(buildFlags, app.RemoteCacheBazelFlags()...)

	result := bazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	assert.NotContains(t, result.Stderr, "1 remote cache hit")

	bazel.Clean(ctx, t, ws)

	result = bazel.Invoke(ctx, t, ws, "build", "//:hello.txt")

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	assert.Contains(t, result.Stderr, "1 remote cache hit")
}
