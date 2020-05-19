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
	"net/url"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
	"google.golang.org/grpc/credentials"
)

type CertCache struct {
	cache interfaces.Cache
}

func (c *CertCache) Get(ctx context.Context, key string) ([]byte, error) {
	bytes, err := c.cache.Get(ctx, key)
	if err != nil {
		return nil, autocert.ErrCacheMiss
	}
	return bytes, nil
}

func (c *CertCache) Put(ctx context.Context, key string, data []byte) error {
	return c.cache.Set(ctx, key, data)
}

func (c *CertCache) Delete(ctx context.Context, key string) error {
	return c.cache.Delete(ctx, key)
}

func NewCertCache(cache interfaces.Cache) *CertCache {
	return &CertCache{
		cache: cache,
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

	httpTLSConfig := &tls.Config{
		NextProtos:               []string{"http/1.1"},
		MinVersion:               tls.VersionTLS10,
		SessionTicketsDisabled:   true,
		PreferServerCipherSuites: true,
	}

	grpcTLSConfig := &tls.Config{
		NextProtos:               []string{"http/1.1"},
		MinVersion:               tls.VersionTLS10,
		SessionTicketsDisabled:   true,
		PreferServerCipherSuites: true,
		ClientAuth:               tls.VerifyClientCertIfGiven,
		ClientCAs:                clientCACertPool,
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
		appURL := s.env.GetConfigurator().GetAppBuildBuddyURL()
		if appURL == "" {
			return status.FailedPreconditionError("No buildbuddy app URL set - unable to use ACME")
		}

		url, err := url.Parse(appURL)
		if err != nil {
			return err
		}
		hosts := []string{url.Hostname()}

		cacheURL := s.env.GetConfigurator().GetAppCacheAPIURL()
		if url, err = url.Parse(cacheURL); cacheURL != "" && err == nil {
			hosts = append(hosts, url.Hostname())
		}

		eventsURL := s.env.GetConfigurator().GetAppEventsAPIURL()
		if url, err = url.Parse(eventsURL); eventsURL != "" && err == nil {
			hosts = append(hosts, url.Hostname())
		}

		s.autocertManager = &autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			Cache:      NewCertCache(s.env.GetCache()),
			HostPolicy: autocert.HostWhitelist(hosts...),
		}
		httpTLSConfig.GetCertificate = s.autocertManager.GetCertificate
		grpcTLSConfig.GetCertificate = s.grpcTLSConfig.GetCertificate
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

func (s *SSLService) ConfigureTLS(mux *http.ServeMux) (*tls.Config, http.Handler) {
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
