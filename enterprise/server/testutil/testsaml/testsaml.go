// Package testsaml provides helpers for using a mock SAML IDP in tests.
package testsaml

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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/mocksaml"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/stretchr/testify/require"
)

// IDP is a mock SAML IDP scoped to a test.
type IDP struct {
	*mocksaml.IDP

	// CertPath is the path to a PEM file containing the certificate that the
	// IDP signs assertions with, suitable for passing to the app via the
	// --auth.saml.trusted_idp_cert_files flag.
	CertPath string
}

// Start starts a mock SAML IDP on a free port and waits for it to become
// ready. The IDP is shut down when the test completes.
func Start(t *testing.T) *IDP {
	idpCert, idpKey := CreateSelfSignedCert(t)
	idp, err := mocksaml.Start(testport.FindFree(t), bytes.NewReader(idpCert), bytes.NewReader(idpKey))
	require.NoError(t, err)
	t.Cleanup(func() { _ = idp.Kill() })
	err = idp.WaitUntilReady(context.Background())
	require.NoError(t, err)
	idpCertFile := testfs.CreateTemp(t)
	_, err = idpCertFile.Write(idpCert)
	require.NoError(t, err)
	_ = idpCertFile.Close()
	return &IDP{IDP: idp, CertPath: idpCertFile.Name()}
}

// CreateSelfSignedCert generates a self-signed cert and returns the
// PEM-encoded certificate and private key.
func CreateSelfSignedCert(t *testing.T) (cert, key []byte) {
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
