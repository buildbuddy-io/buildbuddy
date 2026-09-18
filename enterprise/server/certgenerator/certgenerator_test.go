package main

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/relayauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	cgpb "github.com/buildbuddy-io/buildbuddy/proto/certgenerator"
)

// newTestCA returns a self-signed certificate and its key, both PEM encoded.
// Each tweak edits the template before signing, to build a defective CA.
func newTestCA(t *testing.T, isCA bool, tweaks ...func(*x509.Certificate)) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test Tunnel CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}
	for _, tweak := range tweaks {
		tweak(tmpl)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
}

func newTunnelGenerator(t *testing.T) (g *generator, caPEM string) {
	t.Helper()
	certPEM, keyPEM := newTestCA(t, true /*=isCA*/)
	ca, err := loadTunnelCA("", certPEM, "", keyPEM)
	require.NoError(t, err)
	return &generator{tunnelCA: ca}, certPEM
}

// clientKey stands in for the keypair bbcert generates on the workstation:
// the PEM public key it sends, and the PEM private key it keeps.
func clientKey(t *testing.T) (pubPEM string, keyPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return pkixPEM(t, &priv.PublicKey), pkcs8PEM(t, priv)
}

func pkixPEM(t *testing.T, pub any) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func pkcs8PEM(t *testing.T, priv any) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

var employee = &claims{Email: "vadim@buildbuddy.io", Domain: "buildbuddy.io", EmailVerified: true}

func TestGenerateTunnelCert_RoundTripsThroughTheGatewayVerifier(t *testing.T) {
	g, caPEM := newTunnelGenerator(t)
	pubPEM, keyPEM := clientKey(t)

	rsp := &cgpb.GenerateResponse{}
	require.NoError(t, g.generateTunnelCert(employee, &cgpb.GenerateRequest{TunnelPublicKey: pubPEM}, rsp))
	tc := rsp.GetTunnelCredentials()
	require.NotNil(t, tc)
	require.Equal(t, caPEM, tc.GetCa(), "the CA is returned as loaded")

	// The profile is what the gateway's verifier insists on: the person in
	// the common name, client-auth usage, a leaf, and a validity window that
	// tolerates a gateway clock behind ours.
	block, _ := pem.Decode([]byte(tc.GetClientCert()))
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	require.Equal(t, "vadim@buildbuddy.io", cert.Subject.CommonName)
	require.Contains(t, cert.ExtKeyUsage, x509.ExtKeyUsageClientAuth)
	require.False(t, cert.IsCA)
	require.WithinDuration(t, time.Now().Add(-tunnelNotBeforeSkew), cert.NotBefore, time.Minute)
	require.WithinDuration(t, time.Now().Add(*tunnelCertExpiry), cert.NotAfter, time.Minute)

	// The certificate is for the client's key: signing with that key produces
	// a credential the gateway accepts, and yields the employee's identity.
	signer, err := relayauth.NewSigner([]byte(tc.GetClientCert()), keyPEM)
	require.NoError(t, err)
	const wgKey = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
	cred, err := signer.Sign("gateway.test", wgKey, relayauth.DefaultAssertionLifetime)
	require.NoError(t, err)
	v, err := relayauth.NewVerifier([]byte(tc.GetCa()), "gateway.test")
	require.NoError(t, err)
	id, err := v.Verify(cred)
	require.NoError(t, err)
	require.Equal(t, "vadim@buildbuddy.io", id.Email)
	require.Equal(t, wgKey, id.WireGuardPublicKey)
	require.WithinDuration(t, cert.NotAfter, id.CertNotAfter, time.Second)
}

func TestGenerateTunnelCert_SendsTheGateways(t *testing.T) {
	g, _ := newTunnelGenerator(t)
	gateways, err := parseTunnelGateways([]TunnelGateway{
		{Target: "grpcs://foo.bar.example", Zones: []TunnelZone{{Suffix: "foo.bb.internal", RewriteTo: "cluster.local"}}},
		{Target: "grpc://192.168.8.1:1985", Zones: []TunnelZone{{Suffix: "bloop.boop.bb.internal"}, {Suffix: "beep.boop.bb.internal"}}},
	})
	require.NoError(t, err)
	g.tunnelGateways = gateways
	pubPEM, _ := clientKey(t)

	rsp := &cgpb.GenerateResponse{}
	require.NoError(t, g.generateTunnelCert(employee, &cgpb.GenerateRequest{TunnelPublicKey: pubPEM}, rsp))
	got := rsp.GetTunnelCredentials().GetGateways()
	require.Len(t, got, 2)
	require.Equal(t, "grpcs://foo.bar.example", got[0].GetTarget())
	require.Len(t, got[0].GetZones(), 1)
	require.Equal(t, "foo.bb.internal", got[0].GetZones()[0].GetSuffix())
	require.Equal(t, "cluster.local", got[0].GetZones()[0].GetRewriteTo())
	require.Equal(t, "grpc://192.168.8.1:1985", got[1].GetTarget())
	require.Len(t, got[1].GetZones(), 2)
	require.Equal(t, "beep.boop.bb.internal", got[1].GetZones()[1].GetSuffix())
	require.Empty(t, got[1].GetZones()[1].GetRewriteTo())
}

func TestParseTunnelGateways(t *testing.T) {
	gateways, err := parseTunnelGateways([]TunnelGateway{
		{Target: " grpcs://foo.bar.example ", Zones: []TunnelZone{
			{Suffix: " Foo.bb.internal. "},
			{Suffix: ".bar.baz.bb.internal", RewriteTo: "cluster.local."},
		}},
		{Target: "grpc://192.168.8.1:1985", Zones: []TunnelZone{{Suffix: "qux.bb.internal"}}},
	})
	require.NoError(t, err)
	require.Len(t, gateways, 2)
	require.Equal(t, "grpcs://foo.bar.example", gateways[0].GetTarget())
	zones := gateways[0].GetZones()
	require.Len(t, zones, 2)
	require.Equal(t, "foo.bb.internal", zones[0].GetSuffix(), "normalized the way the client matches names")
	require.Equal(t, "bar.baz.bb.internal", zones[1].GetSuffix())
	require.Equal(t, "cluster.local", zones[1].GetRewriteTo())
	require.Equal(t, "qux.bb.internal", gateways[1].GetZones()[0].GetSuffix())

	alone, err := parseTunnelGateways([]TunnelGateway{{Target: "grpcs://a", Zones: []TunnelZone{{Suffix: "bb.internal"}}}})
	require.NoError(t, err)
	require.Equal(t, "bb.internal", alone[0].GetZones()[0].GetSuffix(), "the parent itself is a valid zone, if it is the only one")

	empty, err := parseTunnelGateways(nil)
	require.NoError(t, err)
	require.Empty(t, empty)

	zone := func(suffix string) []TunnelZone { return []TunnelZone{{Suffix: suffix}} }
	for _, tc := range []struct {
		name    string
		gateway TunnelGateway
		want    string
	}{
		{"a zone outside the parent", TunnelGateway{Target: "grpcs://g", Zones: zone("foo.bar.example")}, "must be under bb.internal"},
		{"the bare TLD", TunnelGateway{Target: "grpcs://g", Zones: zone("internal")}, "must be under bb.internal"},
		{"a lookalike", TunnelGateway{Target: "grpcs://g", Zones: zone("notbb.internal")}, "must be under bb.internal"},
		{"no suffix", TunnelGateway{Target: "grpcs://g", Zones: zone("")}, "suffix is required"},
		{"no zones", TunnelGateway{Target: "grpcs://g"}, "no zones"},
		{"no target", TunnelGateway{Zones: zone("foo.bb.internal")}, "grpc:// or grpcs:// target"},
		{"a target without a scheme", TunnelGateway{Target: "foo.bar.example:443", Zones: zone("foo.bb.internal")}, "grpc:// or grpcs:// target"},
		{"an https target", TunnelGateway{Target: "https://foo.bar.example", Zones: zone("foo.bb.internal")}, "grpc:// or grpcs:// target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseTunnelGateways([]TunnelGateway{tc.gateway})
			require.ErrorContains(t, err, tc.want)
			require.True(t, status.IsFailedPreconditionError(err), "a bad gateway must fail startup")
		})
	}

	// Every name belongs to exactly one gateway: a suffix may not be repeated
	// or nested, in either order, within a gateway or across gateways.
	for _, tc := range []struct {
		name string
		a, b string
	}{
		{"repeated", "foo.bb.internal", "FOO.bb.internal."},
		{"a child of an earlier zone", "bb.internal", "dev.bb.internal"},
		{"a parent of an earlier zone", "dev.bb.internal", "bb.internal"},
		{"deeper nesting", "foo.bb.internal", "a.b.foo.bb.internal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseTunnelGateways([]TunnelGateway{
				{Target: "grpcs://a", Zones: zone(tc.a)},
				{Target: "grpcs://b", Zones: zone(tc.b)},
			})
			require.ErrorContains(t, err, "overlaps zone")
			_, err = parseTunnelGateways([]TunnelGateway{
				{Target: "grpcs://a", Zones: []TunnelZone{{Suffix: tc.a}, {Suffix: tc.b}}},
			})
			require.ErrorContains(t, err, "overlaps zone")
		})
	}
	// Overlap is label-aligned: a shared string suffix is not a shared zone.
	_, err = parseTunnelGateways([]TunnelGateway{
		{Target: "grpcs://a", Zones: zone("foo.bb.internal")},
		{Target: "grpcs://b", Zones: zone("notfoo.bb.internal")},
	})
	require.NoError(t, err)

	_, err = parseTunnelGateways([]TunnelGateway{
		{Target: "grpcs://a", Zones: zone("foo.bb.internal")},
		{Target: "grpcs://a ", Zones: zone("bar.bb.internal")},
	})
	require.ErrorContains(t, err, `tunnel gateway "grpcs://a": listed twice`)
}

func TestLoadTunnelConfig(t *testing.T) {
	gateway := TunnelGateway{Target: "grpcs://foo.bar.example", Zones: []TunnelZone{{Suffix: "foo.bb.internal"}}}

	flags.Set(t, "certgenerator.tunnel.gateways", []TunnelGateway{gateway})
	err := (&generator{}).loadTunnelConfig()
	require.ErrorContains(t, err, "needs a tunnel CA", "gateways without a CA to send them with is a misconfiguration")

	certPEM, keyPEM := newTestCA(t, true /*=isCA*/)
	flags.Set(t, "certgenerator.tunnel.ca", certPEM)
	flags.Set(t, "certgenerator.tunnel.ca_key", keyPEM)
	g := &generator{}
	require.NoError(t, g.loadTunnelConfig())
	require.NotNil(t, g.tunnelCA)
	require.Len(t, g.tunnelGateways, 1)

	flags.Set(t, "certgenerator.tunnel.gateways", []TunnelGateway{{Target: "grpcs://g", Zones: []TunnelZone{{Suffix: "foo.bar.example"}}}})
	require.Error(t, (&generator{}).loadTunnelConfig(), "a bad zone fails startup")

	flags.Set(t, "certgenerator.tunnel.gateways", []TunnelGateway{})
	g = &generator{}
	require.NoError(t, g.loadTunnelConfig())
	require.NotNil(t, g.tunnelCA)
	require.Empty(t, g.tunnelGateways, "a CA with no gateways still issues certificates")
}

func TestGenerateTunnelCert_OnlyForPeople(t *testing.T) {
	// Service accounts pass validateUser with no hosted domain; they get SSH
	// and Kubernetes credentials, not a certificate that authenticates a
	// person to the relay gateway.
	g, _ := newTunnelGenerator(t)
	pubPEM, _ := clientKey(t)
	sa := &claims{Email: "ci@project.iam.gserviceaccount.com", AuthorizedPresenter: "ci@project.iam.gserviceaccount.com", EmailVerified: true}

	rsp := &cgpb.GenerateResponse{}
	require.NoError(t, g.generateTunnelCert(sa, &cgpb.GenerateRequest{TunnelPublicKey: pubPEM}, rsp))
	require.Nil(t, rsp.GetTunnelCredentials())
}

func TestGenerateTunnelCert_NothingWithoutAKeyOrACA(t *testing.T) {
	g, _ := newTunnelGenerator(t)
	rsp := &cgpb.GenerateResponse{}
	require.NoError(t, g.generateTunnelCert(employee, &cgpb.GenerateRequest{}, rsp))
	require.Nil(t, rsp.GetTunnelCredentials(), "a client that sends no key asked for no certificate")

	pubPEM, _ := clientKey(t)
	noCA := &generator{}
	require.NoError(t, noCA.generateTunnelCert(employee, &cgpb.GenerateRequest{TunnelPublicKey: pubPEM}, rsp))
	require.Nil(t, rsp.GetTunnelCredentials(), "a server with no tunnel CA issues nothing")
}

func TestGenerateTunnelCert_RejectsUnusableKeys(t *testing.T) {
	g, _ := newTunnelGenerator(t)

	edPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	p224, err := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	require.NoError(t, err)
	rsa1024, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	require.NoError(t, err)

	for name, pubPEM := range map[string]string{
		"not PEM":              "-----BEGIN NOTHING-----\nAAAA\n-----END NOTHING-----\n",
		"a private key":        string(pkcs8PEM(t, p224)),
		"ed25519, unsupported": pkixPEM(t, edPub),
		"P-224, not P-256":     pkixPEM(t, &p224.PublicKey),
		"P-384, not P-256":     pkixPEM(t, &p384.PublicKey),
		"RSA, not ECDSA":       pkixPEM(t, &rsa1024.PublicKey),
	} {
		t.Run(name, func(t *testing.T) {
			rsp := &cgpb.GenerateResponse{}
			err := g.generateTunnelCert(employee, &cgpb.GenerateRequest{TunnelPublicKey: pubPEM}, rsp)
			require.True(t, status.IsInvalidArgumentError(err), "got %v", err)
			require.Nil(t, rsp.GetTunnelCredentials())
		})
	}
}

func TestLoadTunnelCA_RejectsMisconfiguration(t *testing.T) {
	caCert, caKey := newTestCA(t, true /*=isCA*/)
	_, otherKey := newTestCA(t, true /*=isCA*/)
	leafCert, leafKey := newTestCA(t, false /*=isCA*/)

	_, err := loadTunnelCA("", caCert, "", caKey)
	require.NoError(t, err)

	_, err = loadTunnelCA("", caCert, "", otherKey)
	require.True(t, status.IsFailedPreconditionError(err), "got %v", err)
	require.ErrorContains(t, err, "does not match")

	_, err = loadTunnelCA("", leafCert, "", leafKey)
	require.True(t, status.IsFailedPreconditionError(err), "got %v", err)
	require.ErrorContains(t, err, "not a CA")

	// A CA restricted to client authentication is exactly what we want.
	clientAuthCert, clientAuthKey := newTestCA(t, true /*=isCA*/, func(c *x509.Certificate) {
		c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	})
	_, err = loadTunnelCA("", clientAuthCert, "", clientAuthKey)
	require.NoError(t, err)

	// Each of these parses fine but fails the gateway's chain verification,
	// so it must fail here instead.
	for name, tc := range map[string]struct {
		tweak func(*x509.Certificate)
		want  string
	}{
		"cannot sign":      {func(c *x509.Certificate) { c.KeyUsage = x509.KeyUsageDigitalSignature }, "keyCertSign"},
		"server auth only": {func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth} }, "clientAuth"},
	} {
		t.Run(name, func(t *testing.T) {
			cert, key := newTestCA(t, true /*=isCA*/, tc.tweak)
			_, err := loadTunnelCA("", cert, "", key)
			require.True(t, status.IsFailedPreconditionError(err), "got %v", err)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
