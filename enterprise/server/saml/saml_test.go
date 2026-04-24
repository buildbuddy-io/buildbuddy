package saml_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/beevik/etree"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/crewjam/saml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
)

const testSlug = "saml-int-test-org"

// testIDP is a minimal in-process SAML IdP. It serves its EntityDescriptor
// metadata over HTTP and exposes the underlying *saml.IdentityProvider so
// tests can synthesize signed SAML responses without driving a browser.
type testIDP struct {
	HTTPServer *httptest.Server
	IDP        *saml.IdentityProvider
}

func (i *testIDP) MetadataURL() string { return i.HTTPServer.URL + "/metadata" }

func startTestIDP(t *testing.T) *testIDP {
	t.Helper()
	cert, key := generateSelfSignedCert(t)

	idp := &saml.IdentityProvider{
		Key:         key,
		Certificate: cert,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metadata", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/samlmetadata+xml")
		body, err := xml.Marshal(idp.Metadata())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(body)
	})
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)

	listener, err := url.Parse(httpServer.URL)
	require.NoError(t, err)
	idp.MetadataURL = url.URL{Scheme: listener.Scheme, Host: listener.Host, Path: "/metadata"}
	idp.SSOURL = url.URL{Scheme: listener.Scheme, Host: listener.Host, Path: "/sso"}
	return &testIDP{HTTPServer: httpServer, IDP: idp}
}

func generateSelfSignedCert(t *testing.T) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"buildbuddy-saml-int-test"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert, key
}

func encodeCertPEM(cert *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}

func encodeKeyPEM(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func startBuildBuddy(t *testing.T, idp *testIDP) *app.App {
	t.Helper()
	spCert, spKey := generateSelfSignedCert(t)
	appConfig := buildbuddy_enterprise.DefaultAppConfig(t)
	appConfig.HttpPort = testport.FindFree(t)
	bbURL := fmt.Sprintf("http://localhost:%d", appConfig.HttpPort)
	bb := buildbuddy_enterprise.RunWithConfig(
		t, appConfig, buildbuddy_enterprise.DefaultConfig,
		"--auth.saml.cert="+string(encodeCertPEM(spCert)),
		"--auth.saml.key="+string(encodeKeyPEM(spKey)),
		"--app.build_buddy_url="+bbURL,
	)

	wc := buildbuddy_enterprise.LoginAsDefaultSelfAuthUser(t, bb)
	require.NoError(t, wc.RPC("UpdateGroup", &grpb.UpdateGroupRequest{
		RequestContext: wc.RequestContext,
		Id:             wc.RequestContext.GetGroupId(),
		Name:           "SAML Integration Test Org",
		UrlIdentifier:  testSlug,
	}, &grpb.UpdateGroupResponse{}))
	res := bb.DB().Exec(
		`UPDATE "Groups" SET saml_idp_metadata_url = ? WHERE url_identifier = ?`,
		idp.MetadataURL(), testSlug)
	require.NoError(t, res.Error)
	require.Greaterf(t, res.RowsAffected, int64(0),
		"failed to set saml_idp_metadata_url: no Groups row matched url_identifier=%q", testSlug)
	return bb
}

// fetchSPMetadata GETs BuildBuddy's SAML SP metadata via /auth/saml/metadata.
func fetchSPMetadata(t *testing.T, bb *app.App) *saml.EntityDescriptor {
	t.Helper()
	resp, err := http.Get(bb.HTTPURL() + "/auth/saml/metadata?slug=" + testSlug)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, resp.StatusCode,
		"fetch SP metadata: status=%d body=%s", resp.StatusCode, string(body))
	md := &saml.EntityDescriptor{}
	require.NoError(t, xml.Unmarshal(body, md))
	require.NotEmpty(t, md.SPSSODescriptors, "SP metadata has no SPSSODescriptor")
	return md
}

// makeSignedSAMLResponse builds an IdP-initiated SAML response containing
// the given session, signs it with the IdP key, and returns the
// base64-encoded XML ready to POST to the SP's ACS endpoint.
func makeSignedSAMLResponse(t *testing.T, idp *testIDP, spMetadata *saml.EntityDescriptor, session *saml.Session) string {
	t.Helper()

	spsso := &spMetadata.SPSSODescriptors[0]
	var acsEndpoint *saml.IndexedEndpoint
	for i := range spsso.AssertionConsumerServices {
		if spsso.AssertionConsumerServices[i].Binding == saml.HTTPPostBinding {
			acsEndpoint = &spsso.AssertionConsumerServices[i]
			break
		}
	}
	require.NotNil(t, acsEndpoint, "SP metadata is missing an HTTP-POST ACS endpoint")

	now := time.Now()
	req := &saml.IdpAuthnRequest{
		IDP:         idp.IDP,
		HTTPRequest: httptest.NewRequest("POST", idp.IDP.SSOURL.String(), nil),
		Now:         now,
		Request: saml.AuthnRequest{
			ID:                          "id-test-authn-request",
			IssueInstant:                now,
			Destination:                 idp.IDP.SSOURL.String(),
			Issuer:                      &saml.Issuer{Format: "urn:oasis:names:tc:SAML:2.0:nameid-format:entity", Value: spMetadata.EntityID},
			AssertionConsumerServiceURL: acsEndpoint.Location,
			ProtocolBinding:             saml.HTTPPostBinding,
		},
		ServiceProviderMetadata: spMetadata,
		SPSSODescriptor:         spsso,
		ACSEndpoint:             acsEndpoint,
	}
	require.NoError(t, saml.DefaultAssertionMaker{}.MakeAssertion(req, session))
	require.NoError(t, req.MakeAssertionEl())
	require.NoError(t, req.MakeResponse())

	doc := etree.NewDocument()
	doc.SetRoot(req.ResponseEl)
	xmlStr, err := doc.WriteToString()
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString([]byte(xmlStr))
}

// loginViaSAML performs an IdP-initiated POST to the SP's ACS URL with the
// given (already base64-encoded) SAML response and returns the cookies the
// SP set in response, including the session cookie an authenticated
// request would carry on subsequent calls.
func loginViaSAML(t *testing.T, bb *app.App, samlResponse string) []*http.Cookie {
	t.Helper()
	form := url.Values{
		"SAMLResponse": []string{samlResponse},
		"RelayState":   []string{""},
	}
	resp, err := noFollowClient().PostForm(bb.HTTPURL()+"/auth/saml/acs?slug="+testSlug, form)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Lessf(t, resp.StatusCode, 400,
		"ACS POST failed: status=%d body=%s", resp.StatusCode, string(body))
	return resp.Cookies()
}

func noFollowClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Timeout:       30 * time.Second,
	}
}

func callCreateUser(t *testing.T, bb *app.App, cookies []*http.Cookie) (int, string) {
	t.Helper()
	req, err := http.NewRequest("POST", bb.HTTPURL()+"/rpc/BuildBuddyService/CreateUser", strings.NewReader("{}"))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for _, c := range cookies {
		req.AddCookie(c)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func callGetUser(t *testing.T, bb *app.App, cookies []*http.Cookie) (*uspb.GetUserResponse, error) {
	t.Helper()
	req, err := http.NewRequest("POST", bb.HTTPURL()+"/rpc/BuildBuddyService/GetUser", strings.NewReader("{}"))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for _, c := range cookies {
		req.AddCookie(c)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}
	res := &uspb.GetUserResponse{}
	require.NoErrorf(t, protojson.Unmarshal(body, res), "decode GetUser response: %s", body)
	return res, nil
}

// Test misconfigured IdP with no usable attributes in the attribute statement.
// Without a valid identifier we can't create a unique user identity.
func TestSAMLLogin_NoSubjectAttributes(t *testing.T) {
	idp := startTestIDP(t)
	bb := startBuildBuddy(t, idp)
	spMetadata := fetchSPMetadata(t, bb)

	resp := makeSignedSAMLResponse(t, idp, spMetadata, &saml.Session{NameID: "anon"})
	cookies := loginViaSAML(t, bb, resp)

	_, err := callGetUser(t, bb, cookies)
	require.Error(t, err, "expected GetUser to fail")
	assert.Contains(t, err.Error(), "subject identifier",
		"error response should explain the missing subject")
	assert.Contains(t, err.Error(), "identity provider",
		"error response should mention the IdP so admins know what to fix")
}

// Happy path with the attribute statement including an email identifier.
func TestSAMLLogin_EmailAttribute(t *testing.T) {
	idp := startTestIDP(t)
	bb := startBuildBuddy(t, idp)
	spMetadata := fetchSPMetadata(t, bb)

	session := &saml.Session{
		NameID: "alice",
		CustomAttributes: []saml.Attribute{{
			Name:       "email",
			NameFormat: "urn:oasis:names:tc:SAML:2.0:attrname-format:basic",
			Values:     []saml.AttributeValue{{Type: "xs:string", Value: "alice@example.com"}},
		}},
	}
	resp := makeSignedSAMLResponse(t, idp, spMetadata, session)
	cookies := loginViaSAML(t, bb, resp)

	// First GetUser call returns "user not found" because we haven't
	// CreateUser'd yet; that's expected for SAML logins where the user
	// row is created on demand. Use the result of CreateUser to bootstrap.
	status, body := callCreateUser(t, bb, cookies)
	require.Equalf(t, http.StatusOK, status, "CreateUser failed: body=%s", body)

	got, err := callGetUser(t, bb, cookies)
	require.NoError(t, err)
	assert.Equal(t, "alice@example.com", got.GetDisplayUser().GetEmail(),
		"GetUser email should reflect the SAML email attribute")
}

// Attribute statement contains a username instead of e-mail.
// We can create a user but the user information will not contain an e-mail.
func TestSAMLLogin_UsernameWithoutEmail(t *testing.T) {
	idp := startTestIDP(t)
	bb := startBuildBuddy(t, idp)
	spMetadata := fetchSPMetadata(t, bb)

	session := &saml.Session{
		NameID: "alice",
		CustomAttributes: []saml.Attribute{{
			Name:       "username",
			NameFormat: "urn:oasis:names:tc:SAML:2.0:attrname-format:basic",
			Values:     []saml.AttributeValue{{Type: "xs:string", Value: "alice"}},
		}},
	}
	resp := makeSignedSAMLResponse(t, idp, spMetadata, session)
	cookies := loginViaSAML(t, bb, resp)

	status, body := callCreateUser(t, bb, cookies)
	require.Equalf(t, http.StatusOK, status, "CreateUser failed: body=%s", body)

	got, err := callGetUser(t, bb, cookies)
	require.NoError(t, err)
	assert.Empty(t, got.GetDisplayUser().GetEmail(),
		"GetUser email should be empty when the IdP did not send one")
}
