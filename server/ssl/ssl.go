package ssl

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"

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

func getTLSConfig(env environment.Env, mux *http.ServeMux) (*tls.Config, http.Handler, error) {
	sslConf := env.GetConfigurator().GetSSLConfig()
	tlsConfig := &tls.Config{
		NextProtos:               []string{"http/1.1"},
		MinVersion:               tls.VersionTLS10,
		SessionTicketsDisabled:   true,
		PreferServerCipherSuites: true,
	}

	if sslConf.KeyFile != "" && sslConf.CertFile != "" {
		certPair, err := tls.LoadX509KeyPair(sslConf.CertFile, sslConf.KeyFile)
		if err != nil {
			return nil, nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{certPair}
		return tlsConfig, mux, nil
	} else if sslConf.UseACME {
		appURL := env.GetConfigurator().GetAppBuildBuddyURL()
		if appURL == "" {
			return nil, nil, status.FailedPreconditionError("No buildbuddy app URL set - unable to use ACME")
		}

		url, err := url.Parse(appURL)
		if err != nil {
			return nil, nil, err
		}
		hosts := []string{url.Hostname()}

		cacheURL := env.GetConfigurator().GetAppCacheAPIURL()
		if url, err = url.Parse(cacheURL); cacheURL != "" && err == nil {
			hosts = append(hosts, url.Hostname())
		}

		eventsURL := env.GetConfigurator().GetAppEventsAPIURL()
		if url, err = url.Parse(eventsURL); eventsURL != "" && err == nil {
			hosts = append(hosts, url.Hostname())
		}

		manager := autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			Cache:      NewCertCache(env.GetCache()),
			HostPolicy: autocert.HostWhitelist(hosts...),
		}
		tlsConfig.GetCertificate = manager.GetCertificate
		tlsConfig.NextProtos = append(tlsConfig.NextProtos, acme.ALPNProto)

		return tlsConfig, manager.HTTPHandler(mux), nil
	}
	return nil, nil, status.AbortedError("SSL disabled by config")
}

func IsEnabled(env environment.Env) bool {
	sslConf := env.GetConfigurator().GetSSLConfig()
	return sslConf != nil && sslConf.EnableSSL
}

func ConfigureTLS(env environment.Env, mux *http.ServeMux) (*tls.Config, http.Handler, error) {
	return getTLSConfig(env, mux)
}

func GetGRPCSTLSCreds(env environment.Env) (credentials.TransportCredentials, error) {
	if !IsEnabled(env) {
		return nil, status.AbortedError("SSL disabled by config")
	}
	tlsConfig, _, err := getTLSConfig(env, nil)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsConfig), nil
}
