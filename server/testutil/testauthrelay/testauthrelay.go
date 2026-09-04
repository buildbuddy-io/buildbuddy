// Package testauthrelay mints tunnel certificate authorities and credentials
// for tests, standing in for the real issuer.
package testauthrelay

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/relayauth"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/metadata"
)

// CredentialContext returns a context carrying a tunnel credential for
// wgPubKey scoped to audience, as the workstation daemon would send it.
func CredentialContext(t testing.TB, signer *relayauth.Signer, audience, wgPubKey string) context.Context {
	t.Helper()
	cred, err := signer.Sign(audience, wgPubKey, relayauth.DefaultAssertionLifetime)
	require.NoError(t, err)
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(relayauth.CredentialHeader, cred))
}

// CA is a throwaway certificate authority that issues tunnel client
// certificates in the shape the gateway expects.
type CA struct {
	cert   *x509.Certificate
	key    crypto.Signer
	pem    []byte
	serial int64
}

// NewCA creates a self-signed CA.
func NewCA(t testing.TB) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test Tunnel CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &CA{
		cert: cert,
		key:  key,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// PEM returns the CA certificate, for gateway.cert_auth.ca.
func (ca *CA) PEM() string { return string(ca.pem) }

// IssuePEM returns a client certificate and key for email, both PEM encoded,
// valid until notAfter. A zero notAfter means 12 hours out.
func (ca *CA) IssuePEM(t testing.TB, email string, notAfter time.Time) (certPEM, keyPEM []byte) {
	t.Helper()
	if notAfter.IsZero() {
		notAfter = time.Now().Add(12 * time.Hour)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	ca.serial++
	template := &x509.Certificate{
		SerialNumber: big.NewInt(ca.serial + 100),
		Subject: pkix.Name{
			CommonName:   email,
			Organization: []string{"BuildBuddy Tunnel"},
		},
		EmailAddresses:        []string{email},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, key.Public(), ca.key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

// Signer issues a credential signer for email, valid for 12 hours.
func (ca *CA) Signer(t testing.TB, email string) *relayauth.Signer {
	t.Helper()
	return ca.SignerWithExpiry(t, email, time.Time{})
}

// SignerWithExpiry issues a credential signer whose certificate expires at
// notAfter, which may be in the past.
func (ca *CA) SignerWithExpiry(t testing.TB, email string, notAfter time.Time) *relayauth.Signer {
	t.Helper()
	certPEM, keyPEM := ca.IssuePEM(t, email, notAfter)
	signer, err := relayauth.NewSigner(certPEM, keyPEM)
	require.NoError(t, err)
	return signer
}
