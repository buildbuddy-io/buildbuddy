package subdomains_test

import (
	"fmt"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testsaml"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/mocksaml"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhosts"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/require"
)

const (
	// domain is the domain that the app is served on, with the app itself on
	// the "app" subdomain and orgs on customer subdomains, mirroring the
	// subdomain setup on buildbuddy.io. The hostnames are statically
	// configured in /etc/hosts.
	domain = "buildbuddy.local"

	orgASlug = "org-a"
	orgBSlug = "org-b"
)

// testEnv is a running app with subdomain handling enabled and two orgs set
// up: org A, which uses SAML via a mock IDP, and org B, which doesn't use
// SAML.
type testEnv struct {
	wt *webtester.WebTester

	// appURL is the URL of the app's default "app" subdomain.
	appURL string
	// orgAURL is the URL of org A's subdomain.
	orgAURL string
	// orgBURL is the URL of org B's subdomain.
	orgBURL string
	// orgBHost is the host:port part of orgBURL.
	orgBHost string
	// idpHost is the host:port that the mock SAML IDP is served on.
	idpHost string
}

func setup(t *testing.T) *testEnv {
	// We need to directly connect to the DB in order to set the SAML IDP
	// metadata URL, so only run this test locally for now.
	buildbuddy_enterprise.MarkTestLocalOnly(t)

	testhosts.Add(t, "127.0.0.1",
		"app."+domain,
		orgASlug+"."+domain,
		orgBSlug+"."+domain,
	)

	idp, idpCertPath := testsaml.Start(t)
	idpURL, err := url.Parse(idp.MetadataURL())
	require.NoError(t, err)

	appConfig := buildbuddy_enterprise.DefaultAppConfig(t)
	env := &testEnv{
		appURL:   fmt.Sprintf("http://app.%s:%d", domain, appConfig.HttpPort),
		orgAURL:  fmt.Sprintf("http://%s.%s:%d", orgASlug, domain, appConfig.HttpPort),
		orgBHost: fmt.Sprintf("%s.%s:%d", orgBSlug, domain, appConfig.HttpPort),
		idpHost:  idpURL.Host,
	}
	env.orgBURL = "http://" + env.orgBHost
	appCert, appKey := testsaml.CreateSelfSignedCert(t)
	bb := buildbuddy_enterprise.RunWithConfig(t, appConfig, buildbuddy_enterprise.DefaultConfig,
		"--auth.saml.key="+string(appKey),
		"--auth.saml.cert="+string(appCert),
		"--auth.saml.trusted_idp_cert_files="+idpCertPath,
		// Enable subdomain handling, mirroring the dev/prod config.
		"--app.build_buddy_url="+env.appURL,
		"--app.enable_subdomain_matching=true",
		"--app.default_subdomains=app",
		"--auth.domain_wide_cookies=true",
		"--app.popup_auth_enabled=true",
	)

	wt := webtester.New(t)
	env.wt = wt

	// Temporarily log in with self-auth on the default "app" subdomain to
	// create two orgs: org A (which uses SAML) and org B (which doesn't).
	// Since popup auth is enabled, the login completes asynchronously via a
	// popup window, so wait for the sidebar with a generous timeout.
	wt.Get(env.appURL)
	wt.FindByDebugID("login-button").Click()
	wt.FindWithTimeout(`[debug-id="org-picker"]`, 30*time.Second)
	webtester.UpdateSelectedOrg(wt, env.appURL, "Org A", orgASlug)
	webtester.CreateOrg(wt, env.appURL, "Org B", orgBSlug)
	// Creating an org selects it, which redirects the browser to the new
	// org's subdomain. Wait for the redirect to complete.
	require.Eventually(t, func() bool {
		u, err := url.Parse(wt.CurrentURL())
		return err == nil && u.Host == env.orgBHost
	}, 30*time.Second, 100*time.Millisecond, "never got redirected to org B's subdomain after creating org B")
	// Configure SAML for org A by manually executing a DB query.
	res := bb.DB().Exec(`
		UPDATE "Groups"
		SET saml_idp_metadata_url = ?
		WHERE url_identifier = ?
		`,
		idp.MetadataURL(),
		orgASlug,
	)
	require.NoError(t, res.Error)
	require.Greater(t, res.RowsAffected, int64(0), "no groups matched slug %s", orgASlug)
	webtester.Logout(wt)

	return env
}

// Regression test for the issue where starting (but not completing) a SAML
// login on one org's subdomain would break the login page on every other
// subdomain, redirecting users to the first org's SAML IDP. Repro steps:
//
//  1. Open a new incognito window
//  2. Go to org A's subdomain (org A uses SAML)
//  3. Click the SSO login button, but abandon the login flow without
//     signing in to the IDP
//  4. Go to org B's subdomain
//  5. Org A's SAML login now appears (full screen)
func TestAbandonedSAMLLoginDoesNotAffectOtherSubdomains(t *testing.T) {
	env := setup(t)

	startSSOLogin(t, env)

	// Abandon the SAML login and go to org B's subdomain in the main window.
	env.wt.Get(env.orgBURL)
	requireStaysOnOrgBLoginPage(t, env)
}

// Logging into org A via SAML should not affect other orgs' subdomains,
// neither while the org A session is valid nor after it expires.
func TestSAMLLoginDoesNotAffectOtherSubdomains(t *testing.T) {
	env := setup(t)

	popupWindow := startSSOLogin(t, env)
	completeSSOLogin(t, env, popupWindow)

	// A SAML session is only valid for the org it belongs to, so org B's
	// subdomain should show org B's login page.
	env.wt.Get(env.orgBURL)
	env.wt.FindWithTimeout(`[debug-id="login-button"]`, 10*time.Second)

	// Simulate the org A SAML session expiring. This deletes the session
	// itself but keeps the rest of the SAML login state, such as the cookie
	// remembering which org's SSO the user logs in with.
	expireSAMLSession(t, env)

	// Org B's subdomain should still show org B's login page rather than
	// restarting the SAML login flow with org A's IDP.
	env.wt.Get(env.orgBURL)
	requireStaysOnOrgBLoginPage(t, env)
}

// Once a SAML login has completed, an expired session should still seamlessly
// log back in with the org's IDP on the org's own subdomain and on default
// subdomains, which aren't tied to any particular org.
func TestExpiredSAMLSessionSeamlesslyLogsBackIn(t *testing.T) {
	env := setup(t)

	popupWindow := startSSOLogin(t, env)
	completeSSOLogin(t, env, popupWindow)
	expireSAMLSession(t, env)

	// Returning to org A's own subdomain should automatically restart the
	// SAML login flow with org A's IDP.
	env.wt.Get(env.orgAURL)
	requireEventuallyOnHost(t, env, env.idpHost)

	// Same for the default "app" subdomain.
	env.wt.Get(env.appURL)
	requireEventuallyOnHost(t, env, env.idpHost)
}

// startSSOLogin goes to org A's login page and clicks its SSO login button,
// which starts the SAML login flow in a popup window. It waits for the popup
// to land on the IDP's sign-in page, which guarantees that the app has
// handled the SAML login request, then re-focuses the main window, leaving
// the popup open on the sign-in page. Returns the popup window handle.
func startSSOLogin(t *testing.T, env *testEnv) (popupWindow string) {
	wt := env.wt
	wt.Get(env.orgAURL)
	mainWindow := wt.CurrentWindowHandle()
	wt.FindWithTimeout(`[debug-id="sso-button"]`, 10*time.Second).Click()
	popupWindow = waitForNewWindow(t, wt, mainWindow)
	wt.SwitchWindow(popupWindow)
	wt.FindWithTimeout(mocksaml.SignInButtonSelector, 10*time.Second)
	wt.SwitchWindow(mainWindow)
	return popupWindow
}

// completeSSOLogin signs in at the IDP in the popup window opened by
// startSSOLogin, then waits for the main window to pick up the completed
// login.
func completeSSOLogin(t *testing.T, env *testEnv, popupWindow string) {
	wt := env.wt
	mainWindow := wt.CurrentWindowHandle()
	wt.SwitchWindow(popupWindow)
	wt.Find(mocksaml.SignInButtonSelector).Click()
	wt.SwitchWindow(mainWindow)
	wt.FindWithTimeout(`[debug-id="org-picker"]`, 30*time.Second)
}

// expireSAMLSession simulates the SAML session expiring by deleting the SAML
// session cookie from the browser.
func expireSAMLSession(t *testing.T, env *testEnv) {
	env.wt.DeleteCookie("token")
}

// requireStaysOnOrgBLoginPage asserts that the browser shows org B's login
// page and stays on it. With the original bug, the app's auto-login logic
// kicks in shortly after the login page loads, picking up org A's slug from
// the domain-wide "Slug" cookie and redirecting the page to org A's SAML
// IDP, so keep watching the page for a few seconds.
func requireStaysOnOrgBLoginPage(t *testing.T, env *testEnv) {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		u, err := url.Parse(env.wt.CurrentURL())
		require.NoError(t, err)
		require.Equal(t, env.orgBHost, u.Host,
			"expected to stay on org B's login page, but got redirected to %q (org A's SAML login?)", u)
		time.Sleep(100 * time.Millisecond)
	}
	// Org B doesn't use SAML, so the plain login button should be shown
	// rather than an SSO button.
	env.wt.Find(`[debug-id="login-button"]`)
	env.wt.AssertNotFound(`[debug-id="sso-button"]`)
}

// requireEventuallyOnHost waits for the browser to land on a URL with the
// given host.
func requireEventuallyOnHost(t *testing.T, env *testEnv, host string) {
	require.Eventually(t, func() bool {
		u, err := url.Parse(env.wt.CurrentURL())
		return err == nil && u.Host == host
	}, 20*time.Second, 100*time.Millisecond, "expected to get redirected to %s", host)
}

// waitForNewWindow waits for a window to be opened that is not in the given
// list of existing window handles, and returns its handle.
func waitForNewWindow(t *testing.T, wt *webtester.WebTester, existing ...string) string {
	var newWindow string
	require.Eventually(t, func() bool {
		for _, handle := range wt.WindowHandles() {
			if !slices.Contains(existing, handle) {
				newWindow = handle
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "timed out waiting for popup window to open")
	return newWindow
}
