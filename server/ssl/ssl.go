package ssl

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/cache_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/events_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
	"google.golang.org/grpc/credentials"
)

var (
	certFile         = flag.String("ssl.cert_file", "", "Path to a PEM encoded certificate file to use for TLS if not using ACME.")
	keyFile          = flag.String("ssl.key_file", "", "Path to a PEM encoded key file to use for TLS if not using ACME.")
	selfSigned       = flag.Bool("ssl.self_signed", false, "If true, a self-signed cert will be generated for TLS termination.")
	clientCACertFile = flag.String("ssl.client_ca_cert_file", "", "Path to a PEM encoded certificate authority file used to issue client certificates for mTLS auth.")
	clientCACert     = flag.String("ssl.client_ca_cert", "", "PEM encoded certificate authority used to issue client certificates for mTLS auth.", flag.Secret)
	clientCAKeyFile  = flag.String("ssl.client_ca_key_file", "", "Path to a PEM encoded certificate authority key file used to issue client certificates for mTLS auth.")
	clientCAKey      = flag.String("ssl.client_ca_key", "", "PEM encoded certificate authority key used to issue client certificates for mTLS auth.", flag.Secret)
	clientCertExp    = flag.Duration("ssl.client_cert_lifespan", 365*100*24*time.Hour, "The duration client certificates are valid for. Ex: '730h' for one month. If not set, defaults to 100 years.")
	hostWhitelist    = flag.Slice("ssl.host_whitelist", []string{}, "Cloud-Only")
	enableSSL        = flag.Bool("ssl.enable_ssl", false, "Whether or not to enable SSL/TLS on gRPC connections (gRPCS).")
	useACME          = flag.Bool("ssl.use_acme", false, "Whether or not to automatically configure SSL certs using ACME. If ACME is enabled, cert_file and key_file should not be set.")
	defaultHost      = flag.String("ssl.default_host", "", "Host name to use for ACME generated cert if TLS request does not contain SNI.")
)

type CertCache struct {
	bs interfaces.Blobstore
}

func (c *CertCache) Get(ctx context.Context, key string) ([]byte, error) {
	bytes, err := c.bs.ReadBlob(ctx, key)
	if err != nil {
		return nil, autocert.ErrCacheMiss
	}
	return bytes, nil
}

func (c *CertCache) Put(ctx context.Context, key string, data []byte) error {
	_, err := c.bs.WriteBlob(ctx, key, data)
	return err
}

func (c *CertCache) Delete(ctx context.Context, key string) error {
	return c.bs.DeleteBlob(ctx, key)
}

func NewCertCache(bs interfaces.Blobstore) *CertCache {
	return &CertCache{
		bs: bs,
	}
}

type SSLService struct {
	env             environment.Env
	httpTLSConfig   *tls.Config
	grpcTLSConfig   *tls.Config
	autocertManager *autocert.Manager
	AuthorityCert   *x509.Certificate
	AuthorityKey    *rsa.PrivateKey
}

func Register(env *real_environment.RealEnv) error {
	sslService, err := NewSSLService(env)
	if err != nil {
		return status.InternalErrorf("Error configuring SSL: %s", err)
	}
	env.SetSSLService(sslService)
	return nil
}

func NewSSLService(env environment.Env) (*SSLService, error) {
	sslService := &SSLService{
		env: env,
	}

	if !sslService.IsEnabled() {
		return sslService, nil
	}

	err := sslService.populateTLSConfig()
	if err != nil {
		return nil, err
	}

	return sslService, nil
}

func (s *SSLService) populateTLSConfig() error {
	clientCACertPool := x509.NewCertPool()

	if (*clientCACertFile != "" || *clientCACert != "") && (*clientCAKeyFile != "" || *clientCAKey != "") {
		if *clientCACertFile != "" && *clientCACert != "" {
			return status.FailedPreconditionError("Authority cert should be specified as a file or directly, but not both")
		}
		if *clientCAKeyFile != "" && *clientCAKey != "" {
			return status.FailedPreconditionError("Authority key should be specified as a file or directly, but not both")
		}

		var certData []byte
		if *clientCACert != "" {
			certData = []byte(*clientCACert)
		} else {
			data, err := os.ReadFile(*clientCACertFile)
			if err != nil {
				return err
			}
			certData = data
		}
		var keyData []byte
		if *clientCAKey != "" {
			keyData = []byte(*clientCAKey)
		} else {
			data, err := os.ReadFile(*clientCAKeyFile)
			if err != nil {
				return err
			}
			keyData = data
		}

		cert, key, err := loadX509KeyPair(certData, keyData)
		if err != nil {
			return err
		}
		s.AuthorityCert = cert
		s.AuthorityKey = key
	}

	if s.AuthorityCert != nil {
		clientCACertPool.AddCert(s.AuthorityCert)
	}

	// List based on Mozilla recommended ciphers:
	// https://wiki.mozilla.org/Security/Server_Side_TLS
	//
	// This matches this list, with all CBC ciphers removed:
	// https://golang.org/src/crypto/tls/cipher_suites.go?s=1340:1374#L40
	//
	// This isn't the default in go for (outdated according to Mozilla) compatibility reasons:
	// https://github.com/golang/go/issues/13385
	cipherSuites := []uint16{
		tls.TLS_AES_128_GCM_SHA256,
		tls.TLS_AES_256_GCM_SHA384,
		tls.TLS_CHACHA20_POLY1305_SHA256,

		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,

		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,

		tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	}

	httpTLSConfig := &tls.Config{
		NextProtos:               []string{"http/1.1"},
		MinVersion:               tls.VersionTLS12,
		SessionTicketsDisabled:   true,
		PreferServerCipherSuites: true,
		CipherSuites:             cipherSuites,
	}

	grpcTLSConfig := &tls.Config{
		NextProtos:               []string{"http/1.1"},
		MinVersion:               tls.VersionTLS12,
		SessionTicketsDisabled:   true,
		PreferServerCipherSuites: true,
		ClientAuth:               tls.VerifyClientCertIfGiven,
		ClientCAs:                clientCACertPool,
		CipherSuites:             cipherSuites,
	}

	if *keyFile != "" && *certFile != "" {
		certPair, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			return err
		}
		httpTLSConfig.Certificates = []tls.Certificate{certPair}
		grpcTLSConfig.Certificates = []tls.Certificate{certPair}
		s.httpTLSConfig = httpTLSConfig
		s.grpcTLSConfig = grpcTLSConfig
	} else if *selfSigned {
		cert, key, err := generateCert(pkix.Name{CommonName: "Server"}, nil)
		if err != nil {
			return err
		}
		certPair, err := tls.X509KeyPair([]byte(cert), []byte(key))
		if err != nil {
			return err
		}
		httpTLSConfig.Certificates = []tls.Certificate{certPair}
		grpcTLSConfig.Certificates = []tls.Certificate{certPair}
		s.httpTLSConfig = httpTLSConfig
		s.grpcTLSConfig = grpcTLSConfig
	} else if *useACME {
		if build_buddy_url.String() == "" {
			return status.FailedPreconditionError("No buildbuddy app URL set - unable to use ACME")
		}
		hosts := []string{build_buddy_url.WithPath("").Hostname()}

		if *hostWhitelist != nil {
			hosts = append(hosts, *hostWhitelist...)
		}

		if cache_api_url.String() != "" {
			hosts = append(hosts, cache_api_url.WithPath("").Hostname())
		}

		if events_api_url.String() != "" {
			hosts = append(hosts, events_api_url.WithPath("").Hostname())
		}

		// Google LB frontend (GFE) doesn't send SNI to backend so we need to provide a default.
		getCert := func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			if hello.ServerName == "" {
				hello.ServerName = *defaultHost
			}
			return s.autocertManager.GetCertificate(hello)
		}

		s.autocertManager = &autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			Cache:      NewCertCache(s.env.GetBlobstore()),
			HostPolicy: autocert.HostWhitelist(hosts...),
			Email:      "security@buildbuddy.io",
		}
		httpTLSConfig.GetCertificate = getCert
		grpcTLSConfig.GetCertificate = getCert
		httpTLSConfig.NextProtos = append(httpTLSConfig.NextProtos, acme.ALPNProto)
		grpcTLSConfig.NextProtos = append(grpcTLSConfig.NextProtos, acme.ALPNProto)

		s.httpTLSConfig = httpTLSConfig
		s.grpcTLSConfig = grpcTLSConfig
	}
	return nil
}

func (s *SSLService) IsEnabled() bool {
	return *enableSSL
}

func (s *SSLService) IsCertGenerationEnabled() bool {
	return s.IsEnabled() && s.AuthorityCert != nil && s.AuthorityKey != nil
}

func (s *SSLService) ConfigureTLS(mux http.Handler) (*tls.Config, http.Handler) {
	if s.autocertManager == nil {
		return s.httpTLSConfig, mux
	}
	return s.httpTLSConfig, s.autocertManager.HTTPHandler(mux)
}

func (s *SSLService) GetGRPCSTLSCreds() (credentials.TransportCredentials, error) {
	if !s.IsEnabled() {
		return nil, status.AbortedError("SSL disabled by config")
	}
	return credentials.NewTLS(s.grpcTLSConfig), nil
}

type CACert struct {
	cert *x509.Certificate
	key  *rsa.PrivateKey
}

// generateCert generates a cert and returns the cert + private key pair.
// An optional CA cert may be specified to sign the generated cert. If omitted,
// the returned cert will be self-signed.
func generateCert(subject pkix.Name, caCert *CACert) (string, string, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}
	notBefore := time.Now()
	notAfter := notBefore.Add(*clientCertExp)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return "", "", err
	}

	template := x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               subject,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		BasicConstraintsValid: true,
	}

	if caCert == nil {
		caCert = &CACert{
			cert: &template,
			key:  priv,
		}
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert.cert, &priv.PublicKey, caCert.key)
	if err != nil {
		return "", "", err
	}
	certBuffer := new(bytes.Buffer)
	if err := pem.Encode(certBuffer, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return "", "", err
	}

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", "", err
	}
	keyBuffer := new(bytes.Buffer)
	if err := pem.Encode(keyBuffer, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
		return "", "", err
	}

	return certBuffer.String(), keyBuffer.String(), nil
}

func (s *SSLService) GenerateCerts(apiKeyID string) (string, string, error) {
	if s.AuthorityCert == nil || s.AuthorityKey == nil {
		return "", "", status.FailedPreconditionError("Cert authority must be setup in order to generate certificiates")
	}

	subject := pkix.Name{
		CommonName:   "BuildBuddy ID",
		SerialNumber: apiKeyID,
	}
	return generateCert(subject, &CACert{cert: s.AuthorityCert, key: s.AuthorityKey})
}

func (s *SSLService) ValidateCert(certString string) (string, string, error) {
	if s.AuthorityCert == nil || s.AuthorityKey == nil {
		return "", "", status.FailedPreconditionError("Cert authority must be setup in order to validate certificiates")
	}

	unescapedCert, err := url.PathUnescape(certString)
	if err != nil {
		return "", "", status.FailedPreconditionErrorf("Failed to unescape client cert: %s", err)
	}

	block, _ := pem.Decode([]byte(unescapedCert))
	if block == nil {
		return "", "", status.FailedPreconditionError("Failed to decode client certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", "", status.FailedPreconditionErrorf("Failed to parse client certificate: %s", err)
	}

	opts := x509.VerifyOptions{
		Roots: s.grpcTLSConfig.ClientCAs,
	}

	if _, err := cert.Verify(opts); err != nil {
		return "", "", status.FailedPreconditionErrorf("Failed to verify client certificate: %s", err)
	}

	return cert.Subject.CommonName, cert.Subject.SerialNumber, nil
}

func loadX509KeyPair(certData, keyData []byte) (*x509.Certificate, *rsa.PrivateKey, error) {
	cpb, _ := pem.Decode(certData)
	kpb, _ := pem.Decode(keyData)
	crt, err := x509.ParseCertificate(cpb.Bytes)
	if err != nil {
		return nil, nil, err
	}

	key, err := x509.ParsePKCS8PrivateKey(kpb.Bytes)
	if err != nil {
		return nil, nil, err
	}

	return crt, key.(*rsa.PrivateKey), nil
}
