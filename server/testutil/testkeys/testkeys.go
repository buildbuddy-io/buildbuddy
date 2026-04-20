package testkeys

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/require"
)

type ES256KeyPair struct {
	PrivateKeyPEM string
	PublicKeyPEM  string
}

func GenerateES256KeyPair(t *testing.T) *ES256KeyPair {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	privateKeyBytes, err := x509.MarshalECPrivateKey(privateKey)
	require.NoError(t, err)
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: privateKeyBytes,
	})

	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	})

	return &ES256KeyPair{
		PrivateKeyPEM: string(privateKeyPEM),
		PublicKeyPEM:  string(publicKeyPEM),
	}
}
