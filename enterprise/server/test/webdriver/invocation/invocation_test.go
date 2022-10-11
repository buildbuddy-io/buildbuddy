package invocation_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthenticatedInvocation_LogUploadEnabled(t *testing.T) {
	wt := webtester.New(t)
	app := buildbuddy_enterprise.Run(t)

	workspacePath := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": "",
		"BUILD":     `genrule(name = "a", outs = ["a.sh"], cmd_bash = "touch $@")`,
	})
	buildArgs := append([]string{
		"//:a",
		"--show_progress=0",
		"--build_metadata=COMMIT_SHA=cc5011e9a82b545885025d5f08b531bfbbf95d5b",
		"--build_metadata=REPO_URL=https://github.com/test-owner/test-repo",
	}, app.BESBazelFlags()...)

	// Log in and get the build flags needed for BuildBuddy, including API key
	webtester.Login(wt, app.HTTPURL())
	buildbuddyBuildFlags := webtester.GetBazelBuildFlags(wt, app.HTTPURL(), webtester.WithEnableCache)
	t.Log(buildbuddyBuildFlags)
	buildArgs = append(buildArgs, buildbuddyBuildFlags...)

	result := testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
	require.NotEmpty(t, result.InvocationID)

	// Make sure we can view the invocation while logged in
	wt.Get(app.HTTPURL() + "/invocation/" + result.InvocationID)

	details := wt.Find(".details").Text()

	assert.Contains(t, details, "Succeeded")
	assert.NotContains(t, details, "Failed")
	assert.Contains(t, details, "//:a")
	assert.Contains(t, details, "Log upload on")
	assert.Contains(t, details, "Remote execution off")

	// Make sure it shows up in repo history
	webtester.ClickSidebarItem(wt, "Repos")

	historyCardTitle := wt.Find(".history .card .title").Text()
	assert.Equal(t, "test-owner/test-repo", historyCardTitle)

	// Make sure it shows up in commit history
	webtester.ClickSidebarItem(wt, "Commits")

	historyCardTitle = wt.Find(".history .card .title").Text()
	assert.Equal(t, "Commit cc5011", historyCardTitle)

	// Sanity check that the login button is not present while logged in,
	// since we rely on this to check whether we're logged out
	require.Empty(
		t, wt.FindAll(".login-button"),
		"login button is not expected to be visible if logged in",
	)

	// Log out and make sure we see only the login page when attempting to view
	// the invocation again

	webtester.Logout(wt)

	wt.Get(app.HTTPURL() + "/invocation/" + result.InvocationID)

	wt.Find(".login-button")

	// TODO(bduffany): Log in as a different self-auth user that is not in the
	// default BB org, and make sure we get PermissionDenied instead of the
	// login page
}
