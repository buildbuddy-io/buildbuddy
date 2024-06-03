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
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/require"
)

func TestSAMLLogin(t *testing.T) {
	// We need to directly connect to the DB in order to set the SAML IDP
	// metadata URL, so only run this test locally for now.
	buildbuddy_enterprise.MarkTestLocalOnly(t)

	// Start mock SAML instance
	ctx := context.Background()
	idpCert, idpKey := createSelfSignedCert(t)
	idp, err := mocksaml.Start(testport.FindFree(t), bytes.NewReader(idpCert), bytes.NewReader(idpKey))
	require.NoError(t, err)
	t.Cleanup(func() { _ = idp.Kill() })
	err = idp.WaitUntilReady(ctx)
	require.NoError(t, err)
	// Start an enterprise app configured to trust the mock SAML instance
	idpCertFile := testfs.CreateTemp(t)
	_, err = idpCertFile.Write(idpCert)
	require.NoError(t, err)
	appCert, appKey := createSelfSignedCert(t)
	wt := webtester.New(t)
	bb := buildbuddy_enterprise.SetupWebTarget(
		t,
		"--auth.saml.key="+string(appKey),
		"--auth.saml.cert="+string(appCert),
		"--auth.saml.trusted_idp_cert_files="+idpCertFile.Name(),
	)

	// Log in with self-auth, then configure an org slug in settings (it's empty
	// by default).
	webtester.Login(wt, bb)
	wt.Get(bb.HTTPURL() + "/settings/org/details")
	const slug = "saml-test"
	wt.Find(`[name="urlIdentifier"]`).SendKeys(slug)
	wt.Find(`.organization-form-submit-button`).Click()

	// Now that the org has a slug, we can set up SAML by manually executing
	// a DB query.
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

	// Now that SAML is configured in the DB and the org has a slug, we should
	// be able to log in externally via the mock SAML IDP.
	webtester.Logout(wt)
	wt.Get(idp.BuildBuddyLoginURL(bb.HTTPURL(), slug))
	wt.Find(mocksaml.SignInButtonSelector).Click()

	// The email that we received from the SAML IDP should now show up in the
	// org picker.
	displayedEmail := wt.Find(`.org-picker-profile-user`).Text()
	require.Equal(t, mocksaml.DefaultUserEmail, displayedEmail)
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
