package ssl

import (
	"crypto/tls"
	"net/http"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
	"google.golang.org/grpc/credentials"
)

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
			return nil, nil, status.FailedPreconditionError("No buildbuddy app URL set")
		}

		url, err := url.Parse(appURL)
		if err != nil {
			return nil, nil, err
		}
		_ = url
		manager := autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			Cache:      env.GetCache(),
			HostPolicy: autocert.HostWhitelist("app.buildbuddy.dev", "cache.buildbuddy.dev", "events.buildbuddy.dev", "buildbuddy.dev"),
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
