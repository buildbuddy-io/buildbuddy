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
	"github.com/stretchr/testify/require"

	cgpb "github.com/buildbuddy-io/buildbuddy/proto/certgenerator"
)

// newTestCA returns a self-signed certificate and its key, both PEM encoded.
func newTestCA(t *testing.T, isCA bool) (certPEM, keyPEM string) {
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
	return &generator{tunnelCA: ca, tunnelCAPEM: certPEM, tunnelNow: time.Now}, certPEM
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
	require.Equal(t, caPEM, tc.GetCa())

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
	v, err := relayauth.NewVerifier([]byte(caPEM), "gateway.test")
	require.NoError(t, err)
	id, err := v.Verify(cred)
	require.NoError(t, err)
	require.Equal(t, "vadim@buildbuddy.io", id.Email)
	require.Equal(t, wgKey, id.WireGuardPublicKey)
	require.WithinDuration(t, cert.NotAfter, id.CertNotAfter, time.Second)
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
}
