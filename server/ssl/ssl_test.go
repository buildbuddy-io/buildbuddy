package ssl_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestSSLService_GenerateAndValidateCerts(t *testing.T) {
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ecKeyBytes, err := x509.MarshalECPrivateKey(ecKey)
	require.NoError(t, err)

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	rsaKeyBytes := x509.MarshalPKCS1PrivateKey(rsaKey)

	tests := []struct {
		name       string
		pemType    string
		keyBytes   []byte
		publicKey  any
		privateKey any
	}{
		{
			name:       "EC_SEC1",
			pemType:    "EC PRIVATE KEY",
			keyBytes:   ecKeyBytes,
			publicKey:  &ecKey.PublicKey,
			privateKey: ecKey,
		},
		{
			name:       "RSA_PKCS1",
			pemType:    "RSA PRIVATE KEY",
			keyBytes:   rsaKeyBytes,
			publicKey:  &rsaKey.PublicKey,
			privateKey: rsaKey,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			caKeyPEM := pem.EncodeToMemory(&pem.Block{Type: tc.pemType, Bytes: tc.keyBytes})

			// Configure client CA with self-signed cert
			caTemplate := &x509.Certificate{
				SerialNumber:          big.NewInt(1),
				Subject:               pkix.Name{CommonName: "Test CA"},
				NotBefore:             time.Now(),
				NotAfter:              time.Now().Add(time.Hour),
				IsCA:                  true,
				KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
				BasicConstraintsValid: true,
			}
			caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, tc.publicKey, tc.privateKey)
			require.NoError(t, err)
			caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
			tmpDir := t.TempDir()
			caCertFile := filepath.Join(tmpDir, "ca-cert.pem")
			caKeyFile := filepath.Join(tmpDir, "ca-key.pem")
			err = os.WriteFile(caCertFile, caCertPEM, 0600)
			require.NoError(t, err)
			err = os.WriteFile(caKeyFile, caKeyPEM, 0600)
			require.NoError(t, err)
			flags.Set(t, "ssl.enable_ssl", true)
			flags.Set(t, "ssl.self_signed", true)
			flags.Set(t, "ssl.client_ca_cert_file", caCertFile)
			flags.Set(t, "ssl.client_ca_key_file", caKeyFile)

			// Create SSLService
			env := testenv.GetTestEnv(t)
			sslService, err := ssl.NewSSLService(env)
			require.NoError(t, err)

			// Generate and validate client cert
			apiKeyID := "test-api-key"
			certPEM, _, err := sslService.GenerateCerts(apiKeyID)
			require.NoError(t, err)

			commonName, serialNumber, err := sslService.ValidateCert(certPEM)
			require.NoError(t, err)
			require.Equal(t, "BuildBuddy ID", commonName)
			require.Equal(t, apiKeyID, serialNumber)
		})
	}
}

func TestLoadCertificateKey_Errors(t *testing.T) {
	tests := []struct {
		name        string
		keyFile     string
		keyString   string
		fileContent []byte
		wantErrMsg  string
	}{
		{
			name:       "missing_args",
			keyFile:    "",
			keyString:  "",
			wantErrMsg: "must be specified",
		},
		{
			name:       "both_args",
			keyFile:    "/some/file",
			keyString:  "some-key-data",
			wantErrMsg: "not both",
		},
		{
			name:        "invalid_pem",
			fileContent: []byte("not valid pem data"),
			wantErrMsg:  "valid PEM data",
		},
		{
			name:        "unsupported_format",
			fileContent: pem.EncodeToMemory(&pem.Block{Type: "UNKNOWN KEY", Bytes: []byte("garbage")}),
			wantErrMsg:  "unsupported key format",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			keyFile := tc.keyFile
			if tc.fileContent != nil {
				keyFile = filepath.Join(t.TempDir(), "key.pem")
				err := os.WriteFile(keyFile, tc.fileContent, 0600)
				require.NoError(t, err)
			}

			_, err := ssl.LoadCertificateKey(keyFile, tc.keyString)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErrMsg)
		})
	}
}
