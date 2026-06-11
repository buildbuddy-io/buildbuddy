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

// Regression test for the issue where starting (but not completing) a SAML
// login on one org's subdomain would break the login page on every other
// subdomain, redirecting users to the first org's SAML IDP. Repro steps:
//
//  1. Open a new incognito window
//  2. Go to org A's subdomain (org A uses SAML)
//  3. Click the SSO login button - it should open a pop-up window. Just
//     close the pop-up
//  4. Go to org B's subdomain
//  5. Org A's SAML login now appears (full screen)
func TestAbandonedSAMLLoginDoesNotAffectOtherSubdomains(t *testing.T) {
	// We need to directly connect to the DB in order to set the SAML IDP
	// metadata URL, so only run this test locally for now.
	buildbuddy_enterprise.MarkTestLocalOnly(t)

	testhosts.Add(t, "127.0.0.1",
		"app."+domain,
		orgASlug+"."+domain,
		orgBSlug+"."+domain,
	)

	idp, idpCertPath := testsaml.Start(t)

	appConfig := buildbuddy_enterprise.DefaultAppConfig(t)
	appURL := fmt.Sprintf("http://app.%s:%d", domain, appConfig.HttpPort)
	orgAURL := fmt.Sprintf("http://%s.%s:%d", orgASlug, domain, appConfig.HttpPort)
	orgBHost := fmt.Sprintf("%s.%s:%d", orgBSlug, domain, appConfig.HttpPort)
	orgBURL := "http://" + orgBHost
	appCert, appKey := testsaml.CreateSelfSignedCert(t)
	bb := buildbuddy_enterprise.RunWithConfig(t, appConfig, buildbuddy_enterprise.DefaultConfig,
		"--auth.saml.key="+string(appKey),
		"--auth.saml.cert="+string(appCert),
		"--auth.saml.trusted_idp_cert_files="+idpCertPath,
		// Enable subdomain handling, mirroring the dev/prod config.
		"--app.build_buddy_url="+appURL,
		"--app.enable_subdomain_matching=true",
		"--app.default_subdomains=app",
		"--auth.domain_wide_cookies=true",
		"--app.popup_auth_enabled=true",
	)

	wt := webtester.New(t)

	// Temporarily log in with self-auth on the default "app" subdomain to
	// create two orgs: org A (which uses SAML) and org B (which doesn't).
	// Since popup auth is enabled, the login completes asynchronously via a
	// popup window, so wait for the sidebar with a generous timeout.
	wt.Get(appURL)
	wt.FindByDebugID("login-button").Click()
	wt.FindWithTimeout(`[debug-id="org-picker"]`, 30*time.Second)
	webtester.UpdateSelectedOrg(wt, appURL, "Org A", orgASlug)
	webtester.CreateOrg(wt, appURL, "Org B", orgBSlug)
	// Creating an org selects it, which redirects the browser to the new
	// org's subdomain. Wait for the redirect to complete.
	require.Eventually(t, func() bool {
		u, err := url.Parse(wt.CurrentURL())
		require.NoError(t, err)
		return u.Host == orgBHost
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

	// Go to org A's subdomain and click its SSO login button, which starts
	// the SAML login flow in a popup window.
	wt.Get(orgAURL)
	mainWindow := wt.CurrentWindowHandle()
	wt.FindWithTimeout(`[debug-id="sso-button"]`, 10*time.Second).Click()
	popupWindow := waitForNewWindow(t, wt, mainWindow)

	// Wait for the popup to land on the IDP's sign-in page, which guarantees
	// that the app has handled the SAML login request.
	wt.SwitchWindow(popupWindow)
	wt.FindWithTimeout(mocksaml.SignInButtonSelector, 10*time.Second)
	wt.SwitchWindow(mainWindow)

	// Abandon the SAML login and go to org B's subdomain in the main window.
	//
	// Note: the popup is intentionally left open rather than closed as in
	// the original repro steps. By this point the app has already recorded
	// the abandoned login attempt (via cookies), which is what triggers the
	// bug; closing the popup just additionally shows an expected
	// "Authentication popup closed" error banner in the opener window, and
	// the webtester fails any test that displays an error banner.
	wt.Get(orgBURL)

	// Watch the page for a few seconds to make sure we stay on org B's login
	// page: with the original bug, the app's auto-login logic kicks in
	// shortly after the login page loads, picking up org A's slug from the
	// domain-wide "Slug" cookie set during the abandoned login attempt and
	// redirecting the page to org A's SAML IDP.
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		u, err := url.Parse(wt.CurrentURL())
		require.NoError(t, err)
		require.Equal(t, orgBHost, u.Host,
			"expected to stay on org B's login page, but got redirected to %q (org A's SAML login?)", u)
		time.Sleep(100 * time.Millisecond)
	}
	// Org B's login page should be shown. Org B doesn't use SAML, so the
	// plain login button should appear rather than an SSO button.
	wt.Find(`[debug-id="login-button"]`)
	wt.AssertNotFound(`[debug-id="sso-button"]`)
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
