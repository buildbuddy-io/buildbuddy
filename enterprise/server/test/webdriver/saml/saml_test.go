package saml_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testsaml"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/mocksaml"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/require"
)

const (
	slug = "saml-test"
)

func startApp(t *testing.T, idp *testsaml.IDP, extraArgs ...string) buildbuddy_enterprise.WebTarget {
	appCert, appKey := testsaml.CreateSelfSignedCert(t)
	args := append([]string{
		"--auth.saml.key=" + string(appKey),
		"--auth.saml.cert=" + string(appCert),
		"--auth.saml.trusted_idp_cert_files=" + idp.CertPath,
	}, extraArgs...)
	bb := buildbuddy_enterprise.SetupWebTarget(t, args...)
	return bb
}

// Creates the org with slug "saml-test" and configures the SAML IDP metadata
// URL in the DB.
func setupSAMLTestOrg(t *testing.T, wt *webtester.WebTester, bb buildbuddy_enterprise.WebTarget, idp *testsaml.IDP) {
	// Temporarily log in with self-auth, then configure an org slug in settings
	// (it's empty by default).
	webtester.Login(wt, bb)
	webtester.UpdateSelectedOrg(wt, bb.HTTPURL(), "SAML Test Org", slug, webtester.EnableUserOwnedAPIKeys)
	webtester.Logout(wt)

	// Now that the org has a slug, set up SAML by manually executing a DB
	// query. After this is done, we can use SAML login instead of self-auth.
	res := bb.(*app.App).DB().Exec(`
		UPDATE "Groups"
		SET saml_idp_metadata_url = ?
		WHERE url_identifier = ?
		`,
		idp.MetadataURL(),
		slug,
	)
	require.NoError(t, res.Error)
	require.Greater(t, res.RowsAffected, int64(0), "no groups matched slug %s", slug)
}

func TestSAMLBasicLogin(t *testing.T) {
	// We need to directly connect to the DB in order to set the SAML IDP
	// metadata URL, so only run this test locally for now.
	buildbuddy_enterprise.MarkTestLocalOnly(t)

	idp := testsaml.Start(t)
	bb := startApp(t, idp)
	wt := webtester.New(t)
	setupSAMLTestOrg(t, wt, bb, idp)

	// Log into the org using SSO login.
	wt.Get(idp.BuildBuddyLoginURL(bb.HTTPURL(), slug))
	wt.Find(mocksaml.SignInButtonSelector).Click()

	// The email that we received from the SAML IDP should now show up in the
	// org picker.
	displayedEmail := wt.Find(`.org-picker-profile-user`).Text()
	require.Equal(t, mocksaml.DefaultUserEmail, displayedEmail)
}

func TestSAMLViewInvocation(t *testing.T) {
	// We need to directly connect to the DB in order to set the SAML IDP
	// metadata URL, so only run this test locally for now.
	buildbuddy_enterprise.MarkTestLocalOnly(t)

	ctx := context.Background()
	idp := testsaml.Start(t)
	// Disable persisting artifacts in blobstore to exercise that cache auth
	// accesses via the UI work when logged in with SAML.
	bb := startApp(t, idp, "--storage.disable_persist_cache_artifacts=true")
	wt := webtester.New(t)
	setupSAMLTestOrg(t, wt, bb, idp)

	// Log into the org using SSO login.
	wt.Get(idp.BuildBuddyLoginURL(bb.HTTPURL(), slug))
	wt.Find(mocksaml.SignInButtonSelector).Click()
	// Wait until we land back at the history page.
	wt.Find(`.history`)

	// Ensure the logged in user has a personal API key.
	webtester.GetOrCreatePersonalAPIKey(wt, bb.HTTPURL())
	// Read the bazel flags from the Quickstart page, then run an authenticated
	// build.
	bazelFlags := webtester.GetBazelBuildFlags(wt, bb.HTTPURL(), webtester.WithEnableCache)
	ws := testbazel.MakeTempModule(t, map[string]string{
		"BUILD": `genrule(name = "foo", outs = ["foo.txt"], cmd_bash = "touch $@")`,
	})
	buildArgs := append([]string{":foo"}, bazelFlags...)
	buildResult := testbazel.Invoke(ctx, t, ws, "build", buildArgs...)

	// Go to the invocation page and make sure we can see the timing profile.
	wt.Get(bb.HTTPURL() + "/invocation/" + buildResult.InvocationID)
	wt.Find(`[href="#timing"]`).Click()
	wt.Find(`.trace-viewer`)
}
