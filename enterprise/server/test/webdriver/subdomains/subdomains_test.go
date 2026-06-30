package subdomains_test

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testsaml"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/mocksaml"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhosts"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/require"
)

type testEnv struct {
	wt *webtester.WebTester
	bb *app.App

	// App HTTP port
	port int
	// mocksaml host:port
	samlIDPHost string
}

// Does a basic SAML login into org-a (which has SAML enabled).
func TestBasicSAMLLogin(t *testing.T) {
	env := setup(t)
	wt := env.wt

	// Go to org A's subdomain and complete a login via its SAML IDP.
	wt.Get(fmt.Sprintf("http://org-a.buildbuddy.local:%d", env.port))
	popupWindow := startSSOLogin(t, wt)
	completeSSOLogin(wt, popupWindow)

	// We should be logged in to org A, as shown by the org picker in the sidebar.
	wt.Find(`[debug-id="org-picker"]`)
}

// Does a basic self-auth (non-SAML) login on the default "app" subdomain.
func TestBasicSelfAuthLogin(t *testing.T) {
	env := setup(t)
	wt := env.wt

	// Click the login button on the app subdomain to log in via self-auth.
	wt.Get(fmt.Sprintf("http://app.buildbuddy.local:%d", env.port))
	wt.FindByDebugID("login-button").Click()

	// We should be logged in, as shown by the org picker in the sidebar. The
	// login completes asynchronously via a popup, so allow a generous timeout.
	wt.FindWithTimeout(`[debug-id="org-picker"]`, 30*time.Second)
}

// A SAML-enabled org's login page should offer SSO login, while a non-SAML
// org's login page should not.
func TestLoginPageOffersSSOForSAMLOrgOnly(t *testing.T) {
	env := setup(t)
	wt := env.wt

	// Org A uses SAML, so its login page should offer SSO login.
	wt.Get(fmt.Sprintf("http://org-a.buildbuddy.local:%d", env.port))
	wt.Find(`[debug-id="sso-button"]`)

	// Org B doesn't use SAML, so its login page should show the self-auth login
	// button and no SSO button.
	wt.Get(fmt.Sprintf("http://org-b.buildbuddy.local:%d", env.port))
	wt.Find(`[debug-id="login-button"]`)
	wt.AssertNotFound(`[debug-id="sso-button"]`)
}

// Logging out should destroy the session, so revisiting the app afterwards
// requires logging in again.
func TestLogoutClearsSession(t *testing.T) {
	env := setup(t)
	wt := env.wt

	// Log in via self-auth on the app subdomain.
	appURL := fmt.Sprintf("http://app.buildbuddy.local:%d", env.port)
	wt.Get(appURL)
	wt.FindByDebugID("login-button").Click()
	wt.FindWithTimeout(`[debug-id="org-picker"]`, 30*time.Second)

	// Log out, then revisit the app subdomain. We should be back on the login
	// page, with no org picker (i.e. logged out).
	webtester.Logout(wt)
	wt.Get(appURL)
	wt.Find(`[debug-id="login-button"]`)
	wt.AssertNotFound(`[debug-id="org-picker"]`)
}

// A logged-out user visiting a subdomain should be shown its login page rather
// than app content.
func TestLoggedOutUserSeesLoginPage(t *testing.T) {
	env := setup(t)
	wt := env.wt

	// setup leaves the browser logged out. The default "app" subdomain and a
	// non-SAML org's subdomain should each show a login button and no org picker.
	for _, subdomain := range []string{"app", "org-b"} {
		wt.Get(fmt.Sprintf("http://%s.buildbuddy.local:%d", subdomain, env.port))
		wt.Find(`[debug-id="login-button"]`)
		wt.AssertNotFound(`[debug-id="org-picker"]`)
	}
}

// SAML identities are scoped to a single org, so users who are members of
// multiple orgs (orgs commonly share a SAML IDP) rely on their SSO org's
// session to authenticate them on their other orgs' subdomains. An org A
// SAML session should authenticate the user on org B's subdomain, and once
// the session expires, org B's subdomain should log back in via org A's IDP.
func TestSAMLSessionAuthenticatesOnOtherOrgSubdomains(t *testing.T) {
	env := setup(t)
	wt := env.wt

	// Log in to org A via its SAML IDP.
	wt.Get(fmt.Sprintf("http://org-a.buildbuddy.local:%d", env.port))
	popupWindow := startSSOLogin(t, wt)
	completeSSOLogin(wt, popupWindow)
	addSAMLUserToOrgB(t, env)

	// The org A SAML session should authenticate the user on org B's subdomain.
	wt.Get(fmt.Sprintf("http://org-b.buildbuddy.local:%d", env.port))
	wt.FindWithTimeout(`[debug-id="org-picker"]`, 20*time.Second)

	// Expire the SAML session.
	wt.DeleteCookie("token")

	// Org B's subdomain should now automatically log back in via org A's IDP.
	wt.Get(fmt.Sprintf("http://org-b.buildbuddy.local:%d", env.port))
	requireEventuallyOnHost(t, wt, env.samlIDPHost)

	// Completing the re-login at the IDP should land back on org B's subdomain,
	// logged in.
	wt.Find(mocksaml.SignInButtonSelector).Click()
	requireEventuallyOnHost(t, wt, fmt.Sprintf("org-b.buildbuddy.local:%d", env.port))
	wt.FindWithTimeout(`[debug-id="org-picker"]`, 20*time.Second)
}

// Once a SAML login has completed, an expired session should still seamlessly
// log back in with the org's IDP on the org's own subdomain and on default
// subdomains, which aren't tied to any particular org.
func TestExpiredSAMLSessionSeamlesslyLogsBackIn(t *testing.T) {
	env := setup(t)
	wt := env.wt

	// Log in to org A via its SAML IDP.
	wt.Get(fmt.Sprintf("http://org-a.buildbuddy.local:%d", env.port))
	popupWindow := startSSOLogin(t, wt)
	completeSSOLogin(wt, popupWindow)

	// Expire the SAML session.
	wt.DeleteCookie("token")

	// Returning to org A's own subdomain should automatically restart the
	// SAML login flow with org A's IDP.
	wt.Get(fmt.Sprintf("http://org-a.buildbuddy.local:%d", env.port))
	requireEventuallyOnHost(t, wt, env.samlIDPHost)

	// Same for the default "app" subdomain.
	wt.Get(fmt.Sprintf("http://app.buildbuddy.local:%d", env.port))
	requireEventuallyOnHost(t, wt, env.samlIDPHost)
}

func setup(t *testing.T) *testEnv {
	// We directly connect to the DB in order to set the SAML IDP metadata URL,
	// so only run this test locally for now.
	buildbuddy_enterprise.MarkTestLocalOnly(t)

	// Set up the "app." subdomain and some org-specific subdomains to resolve
	// to localhost.
	testhosts.ResolveToLocalhost(t,
		"app.buildbuddy.local",
		"org-a.buildbuddy.local",
		"org-b.buildbuddy.local",
	)

	idp := testsaml.Start(t)
	idpURL, err := url.Parse(idp.MetadataURL())
	require.NoError(t, err)

	appConfig := buildbuddy_enterprise.DefaultAppConfig(t)
	env := &testEnv{
		port:        appConfig.HttpPort,
		samlIDPHost: idpURL.Host,
	}
	appURL := fmt.Sprintf("http://app.buildbuddy.local:%d", env.port)
	appCert, appKey := testsaml.CreateSelfSignedCert(t)
	bb := buildbuddy_enterprise.RunWithConfig(t, appConfig, buildbuddy_enterprise.DefaultConfig,
		"--auth.saml.key="+string(appKey),
		"--auth.saml.cert="+string(appCert),
		"--auth.saml.trusted_idp_cert_files="+idp.CertPath,
		// Enable subdomain handling, mirroring the dev/prod config.
		"--app.build_buddy_url="+appURL,
		"--app.enable_subdomain_matching=true",
		"--app.default_subdomains=app",
		"--auth.domain_wide_cookies=true",
		// Enable popup auth to match dev/prod.
		"--app.popup_auth_enabled=true",
	)

	env.bb = bb
	wt := webtester.New(t)
	env.wt = wt

	// Temporarily log in with self-auth on the default "app" subdomain to
	// create two orgs: org A, which we configure to use SAML below, and org B,
	// which keeps using self-auth (it never gets a SAML IDP configured).
	wt.Get(appURL)
	wt.FindByDebugID("login-button").Click()

	// Since popup auth is enabled, the login completes asynchronously via a
	// popup window, so wait for the sidebar with a generous timeout.
	wt.FindWithTimeout(`[debug-id="org-picker"]`, 30*time.Second)
	webtester.UpdateSelectedOrg(wt, appURL, "Org A", "org-a")

	// Create org B, which should redirect to org B's subdomain. Wait until the
	// redirect is complete.
	webtester.CreateOrg(wt, appURL, "Org B", "org-b")
	require.Eventually(t, func() bool {
		u, err := url.Parse(wt.CurrentURL())
		return err == nil && u.Host == fmt.Sprintf("org-b.buildbuddy.local:%d", env.port)
	}, 30*time.Second, 100*time.Millisecond, "never got redirected to org B's subdomain after creating org B")

	// Configure SAML for org A by manually executing a DB query.
	res := bb.DB().Exec(`
		UPDATE "Groups"
		SET saml_idp_metadata_url = ?
		WHERE url_identifier = ?
		`,
		idp.MetadataURL(),
		"org-a",
	)
	require.NoError(t, res.Error)
	require.Greater(t, res.RowsAffected, int64(0), "no groups matched slug %s", "org-a")
	webtester.Logout(wt)

	return env
}

// startSSOLogin clicks the SSO login button on the current login page, waits
// for the SAML login page to become visible, then returns a handle on the
// popup.
func startSSOLogin(t *testing.T, wt *webtester.WebTester) (popupWindow string) {
	mainWindow := wt.CurrentWindowHandle()
	wt.FindWithTimeout(`[debug-id="sso-button"]`, 10*time.Second).Click()
	require.Eventually(t, func() bool {
		for _, handle := range wt.WindowHandles() {
			if handle != mainWindow {
				popupWindow = handle
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "timed out waiting for popup window to open")
	wt.SwitchWindow(popupWindow)
	wt.FindWithTimeout(mocksaml.SignInButtonSelector, 10*time.Second)
	wt.SwitchWindow(mainWindow)
	return popupWindow
}

// completeSSOLogin signs in at the IDP in the popup window opened by
// startSSOLogin, then waits for the main window to pick up the completed
// login.
func completeSSOLogin(wt *webtester.WebTester, popupWindow string) {
	mainWindow := wt.CurrentWindowHandle()
	wt.SwitchWindow(popupWindow)
	wt.Find(mocksaml.SignInButtonSelector).Click()
	wt.SwitchWindow(mainWindow)
	wt.FindWithTimeout(`[debug-id="org-picker"]`, 30*time.Second)
}

// addSAMLUserToOrgB makes the SAML user, who became a member of org A by
// logging in via org A's IDP, a member of org B as well, by copying their
// org A membership row.
func addSAMLUserToOrgB(t *testing.T, env *testEnv) {
	res := env.bb.DB().Exec(`
		INSERT INTO "UserGroups" (user_user_id, group_group_id, role, membership_status)
		SELECT ug.user_user_id, gb.group_id, ug.role, ug.membership_status
		FROM "UserGroups" ug
		JOIN "Users" u ON u.user_id = ug.user_user_id
		JOIN "Groups" ga ON ga.group_id = ug.group_group_id
		JOIN "Groups" gb ON gb.url_identifier = ?
		WHERE ga.url_identifier = ? AND u.sub_id LIKE ('%saml/metadata?slug=' || ? || '/%')
		`,
		"org-b",
		"org-a",
		"org-a",
	)
	require.NoError(t, res.Error)
	require.EqualValues(t, 1, res.RowsAffected, "expected to find exactly one org A SAML user membership to copy")
}

// requireEventuallyOnHost waits for the browser to land on a URL with the
// given host.
func requireEventuallyOnHost(t *testing.T, wt *webtester.WebTester, host string) {
	require.Eventually(t, func() bool {
		u, err := url.Parse(wt.CurrentURL())
		return err == nil && u.Host == host
	}, 20*time.Second, 100*time.Millisecond, "expected to get redirected to %s", host)
}
