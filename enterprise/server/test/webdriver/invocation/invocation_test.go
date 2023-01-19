package invocation_test

import (
	"context"
	"flag"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var (
	devSSOSlug  = flag.String("auth.webdriver.dev_slug", "", "Dev SSO slug to login.")
	prodSSOSlug = flag.String("auth.webdriver.prod_slug", "", "Prod SSO slug to login.")
)

type webdriverTarget interface {
	init(t *testing.T)

	appURL() string
	ssoSlug() string
}

type localTarget struct {
	app *app.App
}

func (d *localTarget) init(t *testing.T) {
	// Run a local server with self-auth enabled
	d.app = buildbuddy_enterprise.RunWithConfig(t, buildbuddy_enterprise.DefaultConfig, "--cache.detailed_stats_enabled=true")
}

func (d *localTarget) appURL() string {
	return d.app.HTTPURL()
}

// Because self-auth is enabled, the driver does not need to log in via SSO
func (d *localTarget) ssoSlug() string {
	return ""
}

type devTarget struct{}

func (d *devTarget) init(t *testing.T) {}

func (d *devTarget) appURL() string {
	return "https://app.buildbuddy.dev"
}

func (d *devTarget) ssoSlug() string {
	return *devSSOSlug
}

type prodTarget struct{}

func (d *prodTarget) init(t *testing.T) {}

func (d *prodTarget) appURL() string {
	return "https://app.buildbuddy.io"
}

func (d *prodTarget) ssoSlug() string {
	return *prodSSOSlug
}

func TestAuthenticatedInvocation_CacheEnabled(t *testing.T) {
	targets := []webdriverTarget{
		&localTarget{},
		&devTarget{},
		&prodTarget{},
	}

	eg := errgroup.Group{}
	for _, target := range targets {
		target := target
		eg.Go(func() error {
			wt := webtester.New(t)
			target.init(t)

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

			if target.ssoSlug() != "" {
				webtester.LoginSSO(wt, target.appURL(), target.ssoSlug())
			} else {
				// If the target does not have a SSO slug, assume that it can login via self-auth
				webtester.LoginSelfAuth(wt, target.appURL())
			}

			// Get the build flags needed for BuildBuddy, including API key, bes results url, bes backend, and remote cache
			buildbuddyBuildFlags := webtester.GetBazelBuildFlags(wt, target.appURL(), webtester.WithEnableCache)
			t.Log(buildbuddyBuildFlags)
			buildArgs = append(buildArgs, buildbuddyBuildFlags...)

			testbazel.Clean(context.Background(), t, workspacePath)
			result := testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
			require.NotEmpty(t, result.InvocationID)

			// Make sure we can view the invocation while logged in
			wt.Get(target.appURL() + "/invocation/" + result.InvocationID)

			details := wt.Find(".details").Text()
			assert.Contains(t, details, "Succeeded")
			assert.NotContains(t, details, "Failed")
			assert.Contains(t, details, "//:a")
			assert.Contains(t, details, "Cache on")
			assert.Contains(t, details, "Remote execution off")

			// Make sure we can view the cache section
			wt.Find(".cache-sections")
			wt.Find("#filter-cache-requests").SendKeys("All")
			cacheRequestsCard := wt.Find(".results-table").Text()
			assert.Contains(t, cacheRequestsCard, "Miss")
			assert.Contains(t, cacheRequestsCard, "Write")
			assert.NotContains(t, cacheRequestsCard, "Hit")

			// Second build of the same target
			testbazel.Clean(context.Background(), t, workspacePath)
			result = testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
			require.NotEmpty(t, result.InvocationID)

			wt.Get(target.appURL() + "/invocation/" + result.InvocationID)

			details = wt.Find(".details").Text()
			assert.Contains(t, details, "Succeeded")
			assert.NotContains(t, details, "Failed")
			assert.Contains(t, details, "//:a")
			assert.Contains(t, details, "Cache on")
			assert.Contains(t, details, "Remote execution off")

			// Cache section should contain a cache hit
			wt.Find(".cache-sections")
			wt.Find("#filter-cache-requests").SendKeys("All")
			cacheRequestsCard = wt.Find(".results-table").Text()
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

			wt.Get(target.appURL() + "/invocation/" + result.InvocationID)

			wt.Find(".login-button")

			// TODO(bduffany): Log in as a different self-auth user that is not in the
			// default BB org, and make sure we get PermissionDenied instead of the
			// login page
			return nil
		})
	}
	eg.Wait()
}
