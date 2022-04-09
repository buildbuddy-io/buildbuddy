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
	"io/ioutil"
	"math/big"
	"net/http"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/cache_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/events_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
	"google.golang.org/grpc/credentials"
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

	sslConf := s.env.GetConfigurator().GetSSLConfig()
	if sslConf.ClientCACertFile != "" && sslConf.ClientCAKeyFile != "" {
		cert, key, err := loadX509KeyPair(sslConf.ClientCACertFile, sslConf.ClientCAKeyFile)
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

	if sslConf.KeyFile != "" && sslConf.CertFile != "" {
		certPair, err := tls.LoadX509KeyPair(sslConf.CertFile, sslConf.KeyFile)
		if err != nil {
			return err
		}
		httpTLSConfig.Certificates = []tls.Certificate{certPair}
		grpcTLSConfig.Certificates = []tls.Certificate{certPair}
		s.httpTLSConfig = httpTLSConfig
		s.grpcTLSConfig = grpcTLSConfig
	} else if sslConf.UseACME {
		if build_buddy_url.BuildBuddyURLString() == "" {
			return status.FailedPreconditionError("No buildbuddy app URL set - unable to use ACME")
		}
		hosts := []string{build_buddy_url.BuildBuddyURL("").Hostname()}

		if sslConf.HostWhitelist != nil {
			hosts = append(hosts, sslConf.HostWhitelist...)
		}

		if cache_api_url.CacheAPIURLString() != "" {
			hosts = append(hosts, cache_api_url.CacheAPIURL("").Hostname())
		}

		if events_api_url.EventsAPIURLString() != "" {
			hosts = append(hosts, events_api_url.EventsAPIURL("").Hostname())
		}

		// Google LB frontend (GFE) doesn't send SNI to backend so we need to provide a default.
		getCert := func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			if hello.ServerName == "" {
				hello.ServerName = sslConf.DefaultHost
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
	sslConf := s.env.GetConfigurator().GetSSLConfig()
	return sslConf != nil && sslConf.EnableSSL
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

func (s *SSLService) GenerateCerts(apiKey string) (string, string, error) {
	if s.AuthorityCert == nil || s.AuthorityKey == nil {
		return "", "", status.FailedPreconditionError("Cert authority must be setup in order to generate certificiates")
	}

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}
	notBefore := time.Now()
	notAfter := notBefore.Add(100 * 365 * 24 * time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return "", "", err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   "BuildBuddy API Key",
			SerialNumber: apiKey,
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, s.AuthorityCert, &priv.PublicKey, s.AuthorityKey)
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

func loadX509KeyPair(certFile, keyFile string) (*x509.Certificate, *rsa.PrivateKey, error) {
	cf, err := ioutil.ReadFile(certFile)
	if err != nil {
		return nil, nil, err
	}

	kf, err := ioutil.ReadFile(keyFile)
	if err != nil {
		return nil, nil, err
	}

	cpb, _ := pem.Decode(cf)
	kpb, _ := pem.Decode(kf)
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
