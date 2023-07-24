package invocation_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/require"
)

func TestAuthenticatedInvocation_CacheEnabled(t *testing.T) {
	wt := webtester.New(t)
	target := buildbuddy_enterprise.SetupWebTarget(t)

	workspacePath := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": "",
		"BUILD":     `genrule(name = "a", outs = ["a.sh"], cmd_bash = "touch $@")`,
	})
	buildArgs := []string{
		"//:a",
		"--show_progress=0",
		"--build_metadata=COMMIT_SHA=cc5011e9a82b545885025d5f08b531bfbbf95d5b",
		"--build_metadata=REPO_URL=https://github.com/test-owner/test-repo",
		"--remote_upload_local_results=1",
	}

	webtester.Login(wt, target)

	// Get the build flags needed for BuildBuddy, including API key, bes results url, bes backend, and remote cache
	setupPageOpts := []webtester.SetupPageOption{
		webtester.WithEnableCache,
	}

	// Don't use a personal API key if enabled, because they don't write AC results, and won't result in a cache hit
	// with the second build
	wt.Get(target.HTTPURL() + "/settings/org/details")
	checkbox := wt.Find(`[name="userOwnedKeysEnabled"]`)
	personalKeysEnabled := checkbox.IsSelected()
	if personalKeysEnabled {
		setupPageOpts = append(setupPageOpts, webtester.WithAPIKeySelection("Default"))
	}

	buildbuddyBuildFlags := webtester.GetBazelBuildFlags(
		wt, target.HTTPURL(),
		setupPageOpts...,
	)
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
	require.Contains(t, details, "Succeeded")
	require.NotContains(t, details, "Failed")
	require.Contains(t, details, "//:a")
	require.Contains(t, details, "Cache on")
	require.Contains(t, details, "Remote execution off")

	// Make sure we can view the cache section
	wt.FindByDebugID("cache-sections")
	wt.FindByDebugID("filter-cache-requests").SendKeys("All")
	cacheRequestsCard := wt.FindByDebugID("cache-results-table").Text()
	require.Contains(t, cacheRequestsCard, "Write")
	require.NotContains(t, cacheRequestsCard, "Hit")

	// Second build of the same target
	testbazel.Clean(context.Background(), t, workspacePath)
	result = testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
	require.NotEmpty(t, result.InvocationID)

	wt.Get(target.HTTPURL() + "/invocation/" + result.InvocationID)

	details = wt.FindByDebugID("invocation-details").Text()
	require.Contains(t, details, "Succeeded")
	require.NotContains(t, details, "Failed")
	require.Contains(t, details, "//:a")
	require.Contains(t, details, "Cache on")
	require.Contains(t, details, "Remote execution off")

	// Cache section should contain a cache hit
	wt.FindByDebugID("cache-sections")
	wt.FindByDebugID("filter-cache-requests").SendKeys("All")
	cacheRequestsCard = wt.FindByDebugID("cache-results-table").Text()
	require.Contains(t, cacheRequestsCard, "Hit")

	// Make sure it shows up in repo history
	webtester.ClickSidebarItem(wt, "Repos")

	historyCardTitle := wt.Find(".history .card .title").Text()
	require.Equal(t, "test-owner/test-repo", historyCardTitle)

	// Make sure it shows up in commit history
	webtester.ClickSidebarItem(wt, "Commits")

	historyCardTitle = wt.Find(".history .card .title").Text()
	require.Equal(t, "Commit cc5011", historyCardTitle)

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

func TestAuthenticatedInvocation_PersonalAPIKey_CacheEnabled(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	wt := webtester.New(t)
	target := buildbuddy_enterprise.SetupWebTarget(t)

	workspacePath := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": "",
		"BUILD":     `genrule(name = "a", outs = ["a.sh"], cmd_bash = "touch $@")`,
	})
	buildArgs := []string{
		"//:a",
		"--show_progress=0",
		"--build_metadata=COMMIT_SHA=cc5011e9a82b545885025d5f08b531bfbbf95d5b",
		"--build_metadata=REPO_URL=https://github.com/test-owner/test-repo",
		"--remote_upload_local_results=1",
	}

	webtester.Login(wt, target)

	// Enable personal API keys
	wt.Get(target.HTTPURL() + "/settings/org/details")
	checkbox := wt.Find(`[name="userOwnedKeysEnabled"]`)
	if !checkbox.IsSelected() {
		// A slug is required, otherwise we can't save the org settings.
		slug := wt.Find(`[name="urlIdentifier"]`)
		if slug.Text() == "" {
			slug.SendKeys("test-slug")
		}
		checkbox.Click()
		wt.Find(`.organization-form-submit-button`).Click()
		wt.Find(`.form-success-message`)
	}
	// Create a personal API key with CAS-only permissions
	wt.Find(`[href="/settings/personal/api-keys"]`).Click()
	existingKeys := wt.FindAll(`.api-key-value`)
	apiKey := ""
	if len(existingKeys) > 0 {
		apiKey = existingKeys[0].Text()
	} else {
		wt.FindByDebugID("create-new-api-key").Click()
		wt.Find(`.dialog-wrapper [name="label"]`).SendKeys("test-personal-key")
		wt.FindByDebugID("cas-only-radio-button").Click()
		wt.Find(`.dialog-wrapper button[type="submit"]`).Click()
		wt.Find(`.api-key-value-hide`).Click()
		apiKey = wt.Find(`.api-key-value`).Text()
	}

	// Get the build flags for BES + cache, using the personal API key
	buildbuddyBuildFlags := webtester.GetBazelBuildFlags(
		wt, target.HTTPURL(),
		webtester.WithEnableCache,
		webtester.WithAPIKeySelection("test-personal-key (personal key)"))
	require.Contains(t, buildbuddyBuildFlags, "--remote_header=x-buildbuddy-api-key="+apiKey)
	t.Log(buildbuddyBuildFlags)
	buildArgs = append(buildArgs, buildbuddyBuildFlags...)

	// Use a unique remote instance name to ensure that an AC write would happen
	// if user-level keys had AC write perms.
	buildArgs = append(buildArgs, fmt.Sprintf("--remote_instance_name=%d", rand.Int63()))
	testbazel.Clean(context.Background(), t, workspacePath)
	result := testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
	require.NotEmpty(t, result.InvocationID)

	// Make sure we can view the invocation while logged in
	wt.Get(target.HTTPURL() + "/invocation/" + result.InvocationID)

	details := wt.FindByDebugID("invocation-details").Text()
	require.Contains(t, details, "Succeeded")
	require.NotContains(t, details, "Failed")
	require.Contains(t, details, "//:a")
	require.Contains(t, details, "Cache on")
	require.Contains(t, details, "Remote execution off")

	// Make sure we can view the cache section
	wt.FindByDebugID("cache-sections")
	wt.FindByDebugID("filter-cache-requests").SendKeys("All")
	rows := getCacheRequestRows(wt)
	require.NotEmpty(t, rows, "expected at least one cache request")
	for _, row := range rows {
		require.Contains(t, []string{"CAS", "AC"}, row.CacheType)
		require.Contains(t, []string{"Hit", "Miss", "Write"}, row.Status)
		if row.CacheType == "AC" {
			require.NotEqual(
				t, row.Status, "Write",
				"found AC Write row, but personal API keys should not have AC write capability")
		}
	}

	// Log out and make sure we see only the login page when attempting to view
	// the invocation again
	webtester.Logout(wt)
	wt.Get(target.HTTPURL() + "/invocation/" + result.InvocationID)
	wt.FindByDebugID("login-button")
}

type CacheRequestRow struct {
	CacheType, Status string
}

func getCacheRequestRows(wt *webtester.WebTester) []*CacheRequestRow {
	var rows []*CacheRequestRow
	elements := wt.FindAll(`[debug-id="cache-results-table"] .result-row`)
	for _, el := range elements {
		rows = append(rows, &CacheRequestRow{
			CacheType: el.Find(`.cache-type-column`).Text(),
			Status:    el.Find(`.status-column`).Text(),
		})
	}
	return rows
}
