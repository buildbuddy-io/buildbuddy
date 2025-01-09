package saml_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/mocksaml"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/require"
)

const (
	slug = "saml-test"
)

func startIDP(t *testing.T) (_ *mocksaml.IDP, certPath string) {
	idpCert, idpKey := createSelfSignedCert(t)
	idp, err := mocksaml.Start(testport.FindFree(t), bytes.NewReader(idpCert), bytes.NewReader(idpKey))
	require.NoError(t, err)
	t.Cleanup(func() { _ = idp.Kill() })
	err = idp.WaitUntilReady(context.Background())
	require.NoError(t, err)
	idpCertFile := testfs.CreateTemp(t)
	_, err = idpCertFile.Write(idpCert)
	require.NoError(t, err)
	_ = idpCertFile.Close()
	return idp, idpCertFile.Name()
}

func startApp(t *testing.T, idpCertPath string, extraArgs ...string) buildbuddy_enterprise.WebTarget {
	appCert, appKey := createSelfSignedCert(t)
	args := append([]string{
		"--auth.saml.key=" + string(appKey),
		"--auth.saml.cert=" + string(appCert),
		"--auth.saml.trusted_idp_cert_files=" + idpCertPath,
	}, extraArgs...)
	bb := buildbuddy_enterprise.SetupWebTarget(t, args...)
	return bb
}

// Creates the org with slug "saml-test" and configures the SAML IDP metadata
// URL in the DB.
func setupSAMLTestOrg(t *testing.T, wt *webtester.WebTester, bb buildbuddy_enterprise.WebTarget, idp *mocksaml.IDP) {
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

	idp, idpCertPath := startIDP(t)
	bb := startApp(t, idpCertPath)
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
	idp, idpCertPath := startIDP(t)
	// Disable persisting artifacts in blobstore to exercise that cache auth
	// accesses via the UI work when logged in with SAML.
	bb := startApp(t, idpCertPath, "--storage.disable_persist_cache_artifacts=true")
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
	ws := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": "",
		"BUILD":     `genrule(name = "foo", outs = ["foo.txt"], cmd_bash = "touch $@")`,
	})
	buildArgs := append([]string{":foo"}, bazelFlags...)
	buildResult := testbazel.Invoke(ctx, t, ws, "build", buildArgs...)

	// Go to the invocation page and make sure we can see the timing profile.
	wt.Get(bb.HTTPURL() + "/invocation/" + buildResult.InvocationID)
	wt.Find(`[href="#timing"]`).Click()
	wt.Find(`.trace-viewer`)
}

func createSelfSignedCert(t *testing.T) (cert, key []byte) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	certTemplate := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"mocksaml"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, &certTemplate, &certTemplate, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)
	var certBuf, keyBuf bytes.Buffer
	err = pem.Encode(&keyBuf, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	require.NoError(t, err)
	err = pem.Encode(&certBuf, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	require.NoError(t, err)
	return certBuf.Bytes(), keyBuf.Bytes()
}
