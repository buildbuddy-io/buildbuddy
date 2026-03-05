package ssl_test

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/stretchr/testify/require"
)

func TestLoadCertificateKey_RSA_PKCS1(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(rsaKey),
	})

	signer, err := ssl.LoadCertificateKey("", string(keyPEM))
	require.NoError(t, err)
	_, ok := signer.(*rsa.PrivateKey)
	require.True(t, ok, "expected *rsa.PrivateKey, got %T", signer)
}

func TestLoadCertificateKey_RSA_PKCS8(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(rsaKey)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Bytes,
	})

	signer, err := ssl.LoadCertificateKey("", string(keyPEM))
	require.NoError(t, err)
	_, ok := signer.(*rsa.PrivateKey)
	require.True(t, ok, "expected *rsa.PrivateKey, got %T", signer)
}

func TestLoadCertificateKey_Ed25519_PKCS8(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Bytes,
	})

	signer, err := ssl.LoadCertificateKey("", string(keyPEM))
	require.NoError(t, err)
	_, ok := signer.(ed25519.PrivateKey)
	require.True(t, ok, "expected ed25519.PrivateKey, got %T", signer)
}

func TestLoadCertificateKey_EC_SEC1(t *testing.T) {
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	sec1Bytes, err := x509.MarshalECPrivateKey(ecKey)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: sec1Bytes,
	})

	signer, err := ssl.LoadCertificateKey("", string(keyPEM))
	require.NoError(t, err)
	_, ok := signer.(*ecdsa.PrivateKey)
	require.True(t, ok, "expected *ecdsa.PrivateKey, got %T", signer)
}

func TestLoadCertificateKey_SkipsLeadingNonKeyPEMBlocks(t *testing.T) {
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	sec1Bytes, err := x509.MarshalECPrivateKey(ecKey)
	require.NoError(t, err)

	keyPEM := append(
		pem.EncodeToMemory(&pem.Block{Type: "EC PARAMETERS", Bytes: []byte("ignored")}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: sec1Bytes})...,
	)

	signer, err := ssl.LoadCertificateKey("", string(keyPEM))
	require.NoError(t, err)
	_, ok := signer.(*ecdsa.PrivateKey)
	require.True(t, ok, "expected *ecdsa.PrivateKey, got %T", signer)
}

func TestLoadCertificateKey_EC_PKCS8(t *testing.T) {
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(ecKey)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Bytes,
	})

	signer, err := ssl.LoadCertificateKey("", string(keyPEM))
	require.NoError(t, err)
	_, ok := signer.(*ecdsa.PrivateKey)
	require.True(t, ok, "expected *ecdsa.PrivateKey, got %T", signer)
}

func TestLoadCertificateKey_FallbackParsesECKeyWithMismatchedPEMType(t *testing.T) {
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	sec1Bytes, err := x509.MarshalECPrivateKey(ecKey)
	require.NoError(t, err)

	// Common failure mode: EC key material is supplied in SEC1 format, but the PEM
	// header is not "EC PRIVATE KEY", causing PKCS8 parsing to fail.
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: sec1Bytes,
	})

	signer, err := ssl.LoadCertificateKey("", string(keyPEM))
	require.NoError(t, err)
	_, ok := signer.(*ecdsa.PrivateKey)
	require.True(t, ok, "expected *ecdsa.PrivateKey, got %T", signer)
}

func TestLoadCertificateKey_EncryptedNotSupported(t *testing.T) {
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "ENCRYPTED PRIVATE KEY",
		Bytes: []byte("not a real encrypted key"),
	})
	_, err := ssl.LoadCertificateKey("", string(keyPEM))
	require.Error(t, err)
	require.ErrorContains(t, err, "encrypted private keys are not supported")
}
