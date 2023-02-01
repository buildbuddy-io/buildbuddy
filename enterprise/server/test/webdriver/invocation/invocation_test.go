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

func TestAuthenticatedInvocation_CacheEnabled(t *testing.T) {
	wt := webtester.New(t)
	target := buildbuddy_enterprise.SetupWebTarget(t)

	workspacePath := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": "",
		"BUILD":     `genrule(name = "a", outs = ["a.sh"], cmd_bash = "touch $@")`,
	})
	buildArgs := append([]string{
		"//:a",
		"--show_progress=0",
		"--build_metadata=COMMIT_SHA=cc5011e9a82b545885025d5f08b531bfbbf95d5b",
		"--build_metadata=REPO_URL=https://github.com/test-owner/test-repo",
		"--remote_upload_local_results=1",
	})

	webtester.Login(wt, target)

	// Get the build flags needed for BuildBuddy, including API key, bes results url, bes backend, and remote cache
	buildbuddyBuildFlags := webtester.GetBazelBuildFlags(wt, target.HTTPURL(), webtester.WithEnableCache)
	t.Log(buildbuddyBuildFlags)
	buildArgs = append(buildArgs, buildbuddyBuildFlags...)

	// To test that the cache section includes writes, don't use the remote cache for the build
	noRemoteCacheBuildArgs := append(buildArgs, "--noremote_accept_cached")
	testbazel.Clean(context.Background(), t, workspacePath)
	result := testbazel.Invoke(context.Background(), t, workspacePath, "build", noRemoteCacheBuildArgs...)
	require.NotEmpty(t, result.InvocationID)

	// Make sure we can view the invocation while logged in
	wt.Get(target.HTTPURL() + "/invocation/" + result.InvocationID)

	details := wt.FindByDebugID("invocation-details").Text()
	assert.Contains(t, details, "Succeeded")
	assert.NotContains(t, details, "Failed")
	assert.Contains(t, details, "//:a")
	assert.Contains(t, details, "Cache on")
	assert.Contains(t, details, "Remote execution off")

	// Make sure we can view the cache section
	wt.FindByDebugID("cache-sections")
	wt.FindByDebugID("filter-cache-requests").SendKeys("All")
	cacheRequestsCard := wt.FindByDebugID("cache-results-table").Text()
	assert.Contains(t, cacheRequestsCard, "Write")
	assert.NotContains(t, cacheRequestsCard, "Hit")

	// Second build of the same target
	testbazel.Clean(context.Background(), t, workspacePath)
	result = testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
	require.NotEmpty(t, result.InvocationID)

	wt.Get(target.HTTPURL() + "/invocation/" + result.InvocationID)

	details = wt.FindByDebugID("invocation-details").Text()
	assert.Contains(t, details, "Succeeded")
	assert.NotContains(t, details, "Failed")
	assert.Contains(t, details, "//:a")
	assert.Contains(t, details, "Cache on")
	assert.Contains(t, details, "Remote execution off")

	// Cache section should contain a cache hit
	wt.FindByDebugID("cache-sections")
	wt.FindByDebugID("filter-cache-requests").SendKeys("All")
	cacheRequestsCard = wt.FindByDebugID("cache-results-table").Text()
	assert.Contains(t, cacheRequestsCard, "Hit")

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

	wt.Get(target.HTTPURL() + "/invocation/" + result.InvocationID)

	wt.FindByDebugID("login-button")

	// TODO(bduffany): Log in as a different self-auth user that is not in the
	// default BB org, and make sure we get PermissionDenied instead of the
	// login page
}
