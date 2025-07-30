package setup_test

import (
	"context"
	"slices"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvocationPage_SuccessfulInvocation_BESOnly(t *testing.T) {
	app := buildbuddy.Run(t)

	workspacePath := testbazel.MakeTempModule(t, map[string]string{
		"BUILD": `genrule(name = "a", outs = ["a.sh"], cmd_bash = "touch $@")`,
	})
	buildArgs := append([]string{"//:a", "--show_progress=0"}, app.BESBazelFlags()...)
	result := testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
	require.NotEmpty(t, result.InvocationID)

	wt := webtester.New(t)

	wt.Get(app.HTTPURL() + "/invocation/" + result.InvocationID)

	details := wt.Find(".details").Text()

	assert.Contains(t, details, "Succeeded")
	assert.NotContains(t, details, "failed")
	assert.Contains(t, details, "//:a")
	assert.Contains(t, details, "Cache off")
	assert.Contains(t, details, "Remote execution off")

	logs := wt.FindByDebugID("build-logs").Text()

	assert.Contains(t, logs, "Streaming build results to:")
	assert.Contains(t, logs, "Target //:a up-to-date:")
}

func TestInvocationPage_FailedInvocation_BESOnly(t *testing.T) {
	app := buildbuddy.Run(t)

	workspacePath := testbazel.MakeTempModule(t, map[string]string{
		"BUILD": `genrule(name = "a", outs = ["a.sh"], cmd_bash = "exit 1")`,
	})
	buildArgs := append([]string{"//:a", "--show_progress=0"}, app.BESBazelFlags()...)
	result := testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
	require.NotEmpty(t, result.InvocationID)

	wt := webtester.New(t)

	wt.Get(app.HTTPURL() + "/invocation/" + result.InvocationID)

	details := wt.Find(".details").Text()

	assert.NotContains(t, details, "Succeeded")
	assert.Contains(t, details, "Build failed")
	assert.Contains(t, details, "//:a")
	assert.Contains(t, details, "Cache off")
	assert.Contains(t, details, "Remote execution off")

	logs := wt.FindByDebugID("build-logs").Text()

	assert.Contains(t, logs, "Streaming build results to:")
	assert.Contains(t, logs, "Target //:a failed to build")
}

func TestInvocationPage_SuccessfulInvocation_BESAndCache(t *testing.T) {
	app := buildbuddy.Run(t)

	workspacePath := testbazel.MakeTempModule(t, map[string]string{
		"BUILD": `genrule(name = "foo", outs = ["foo.out"], cmd_bash = "printf 'foo' > $@ && echo test-stdout && echo test-stderr >&2")`,
	})
	buildArgs := slices.Concat(
		[]string{"//:foo", "--show_progress=0", "--remote_upload_local_results"},
		app.BESBazelFlags(),
		app.RemoteCacheBazelFlags(),
	)
	result := testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
	require.NotEmpty(t, result.InvocationID)

	wt := webtester.New(t)

	wt.Get(app.HTTPURL() + "/invocation/" + result.InvocationID)

	details := wt.Find(".details").Text()

	assert.Contains(t, details, "Succeeded")
	assert.Contains(t, details, "//:foo")
	assert.Contains(t, details, "Cache on")
	assert.Contains(t, details, "Remote execution off")

	logs := wt.FindByDebugID("build-logs").Text()

	assert.Contains(t, logs, "Streaming build results to:")
	assert.Contains(t, logs, "Target //:foo up-to-date:")

	// Go to cache requests card
	wt.Find(`[href="#cache"]`).Click()
	// Find action links
	actionPageLinks := wt.Find(`[debug-id="cache-results-table"]`).FindAll(`.action-page-link`)
	// Click the first one
	actionPageLinks[0].Click()

	// Should see the genrule action details page, including stdout/stderr
	stderr := wt.Find(`.stderr-container .terminal`).Text()
	assert.Contains(t, stderr, "test-stderr")
	stdout := wt.Find(`.stdout-container .terminal`).Text()
	assert.Contains(t, stdout, "test-stdout")
}
