package ssl_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

type certificateHealthChecker struct {
	interfaces.HealthChecker
	shutdown func(context.Context) error
}

func (h *certificateHealthChecker) RegisterShutdownFunction(f interfaces.CheckerFunc) {
	h.shutdown = f
}

func TestFileCertificateReload(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	writeCertificate := func() tls.Certificate {
		cert, key, err := ssl.GenerateCert(pkix.Name{CommonName: "localhost"}, nil, time.Hour)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(certPath, []byte(cert), 0600))
		require.NoError(t, os.WriteFile(keyPath, []byte(key), 0600))
		pair, err := tls.X509KeyPair([]byte(cert), []byte(key))
		require.NoError(t, err)
		return pair
	}
	first := writeCertificate()
	flags.Set(t, "ssl.enable_ssl", true)
	flags.Set(t, "ssl.cert_file", certPath)
	flags.Set(t, "ssl.key_file", keyPath)
	flags.Set(t, "ssl.cert_reload_interval", 10*time.Second)
	hc := &certificateHealthChecker{}
	env := real_environment.NewRealEnv(hc)
	clock := clockwork.NewFakeClock()
	env.SetClock(clock)
	service, err := ssl.NewSSLService(env)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, hc.shutdown(context.Background())) })
	config, _ := service.ConfigureTLS(nil)
	get := func() *tls.Certificate {
		cert, err := config.GetCertificate(&tls.ClientHelloInfo{})
		require.NoError(t, err)
		return cert
	}
	original := get()
	require.Equal(t, first.Certificate, original.Certificate)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, clock.BlockUntilContext(ctx, 1))
	failures := testutil.ToFloat64(metrics.SSLCertificateReloadFailures)
	require.NoError(t, os.WriteFile(keyPath, []byte("invalid key"), 0600))
	clock.Advance(10 * time.Second)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.SSLCertificateReloadFailures) == failures+1
	}, time.Second, time.Millisecond)
	require.Equal(t, first.Certificate, get().Certificate, "failed reload preserves the previous certificate")
	second := writeCertificate()
	require.Equal(t, first.Certificate, get().Certificate)
	clock.Advance(10 * time.Second)
	require.Eventually(t, func() bool {
		cert, err := config.GetCertificate(&tls.ClientHelloInfo{})
		return err == nil && cert != nil && len(cert.Certificate) > 0 && string(cert.Certificate[0]) == string(second.Certificate[0])
	}, time.Second, time.Millisecond)
	require.Equal(t, second.Certificate, get().Certificate)
	require.Equal(t, first.Certificate, original.Certificate, "previously returned certificates remain unchanged")
}
