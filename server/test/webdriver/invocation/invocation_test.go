package setup_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvocationPage_SuccessfulInvocation_BESOnly(t *testing.T) {
	app := buildbuddy.Run(t)

	workspacePath := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": "",
		"BUILD":     `genrule(name = "a", outs = ["a.sh"], cmd_bash = "touch $@")`,
	})
	buildArgs := append([]string{"//:a", "--show_progress=0"}, app.BESBazelFlags()...)
	result := testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
	require.NotEmpty(t, result.InvocationID)

	wt := webtester.New(t)

	wt.Get(app.HTTPURL() + "/invocation/" + result.InvocationID)

	details := wt.Find(".details").Text()

	assert.Contains(t, details, "Succeeded")
	assert.NotContains(t, details, "Failed")
	assert.Contains(t, details, "//:a")
	assert.Contains(t, details, "Cache off")
	assert.Contains(t, details, "Remote execution off")

	logs := wt.Find(".terminal").Text()

	assert.Contains(t, logs, "Streaming build results to:")
	assert.Contains(t, logs, "Target //:a up-to-date:")
}

func TestInvocationPage_FailedInvocation_BESOnly(t *testing.T) {
	app := buildbuddy.Run(t)

	workspacePath := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": "",
		"BUILD":     `genrule(name = "a", outs = ["a.sh"], cmd_bash = "exit 1")`,
	})
	buildArgs := append([]string{"//:a", "--show_progress=0"}, app.BESBazelFlags()...)
	result := testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
	require.NotEmpty(t, result.InvocationID)

	wt := webtester.New(t)

	wt.Get(app.HTTPURL() + "/invocation/" + result.InvocationID)

	details := wt.Find(".details").Text()

	assert.NotContains(t, details, "Succeeded")
	assert.Contains(t, details, "Failed")
	assert.Contains(t, details, "//:a")
	assert.Contains(t, details, "Cache off")
	assert.Contains(t, details, "Remote execution off")

	logs := wt.Find(".terminal").Text()

	assert.Contains(t, logs, "Streaming build results to:")
	assert.Contains(t, logs, "Target //:a failed to build")
}
