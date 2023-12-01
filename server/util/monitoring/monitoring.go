package monitoring

import (
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/basicauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagz"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	channelz "github.com/rantav/go-grpc-channelz"
)

var (
	basicAuthUser = flag.String("monitoring.basic_auth.username", "", "Optional username for basic auth on the monitoring port.")
	basicAuthPass = flag.String("monitoring.basic_auth.password", "", "Optional password for basic auth on the monitoring port.", flag.Secret)
)

// Registers monitoring handlers on the provided mux. Note that using
// StartMonitoringHandler on a monitoring-only port is preferred.
func RegisterMonitoringHandlers(env environment.Env, mux *http.ServeMux) {
	handle := mux.Handle
	if *basicAuthUser != "" || *basicAuthPass != "" {
		auth := basicauth.Middleware(basicauth.DefaultRealm, map[string]string{*basicAuthUser: *basicAuthPass})
		handle = func(pattern string, handler http.Handler) {
			mux.Handle(pattern, auth(handler))
		}
	}

	// Prometheus metrics
	handle("/metrics", promhttp.Handler())

	// PProf endpoints
	handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

	// Statusz page
	handle("/statusz", statusz.Server())

	// Flagz page
	handle("/flagz", http.HandlerFunc(flagz.ServeHTTP))

	// Channelz page
	handle("/channelz/", channelz.CreateHandler("/", fmt.Sprintf("%s:%d", env.GetListenAddr(), grpc_server.InternalPort())))
	// Redirect "/channelz" to "/channelz/" (so the trailing slash is
	// optional)
	handle("/channelz", http.RedirectHandler("/channelz/", http.StatusFound))
}

// StartMonitoringHandler enables the prometheus and pprof monitoring handlers
// in the same mux on the specified host and port, which should not have
// anything else running on it.
func StartMonitoringHandler(env environment.Env, hostPort string) {
	mux := http.NewServeMux()
	RegisterMonitoringHandlers(env, mux)
	s := &http.Server{
		Addr:    hostPort,
		Handler: http.Handler(mux),
	}

	go func() {
		log.Infof("Enabling monitoring (pprof/prometheus) interface on http://%s", hostPort)
		if err := s.ListenAndServe(); err != nil {
			log.Fatal(err.Error())
		}
	}()
}

func StartSSLMonitoringHandler(env environment.Env, hostPort string) error {
	ssl := env.GetSSLService()
	if !ssl.IsEnabled() {
		return status.InvalidArgumentError("ssl must be enabled in config to use SSL monitoring")
	}
	mux := http.NewServeMux()
	RegisterMonitoringHandlers(env, mux)
	tlsConfig, _ := ssl.ConfigureTLS(mux)
	s := &http.Server{
		Addr:      hostPort,
		Handler:   mux,
		TLSConfig: tlsConfig,
	}

	go func() {
		log.Infof("Enabling monitoring (pprof/prometheus) interface with SSL on https://%s", hostPort)
		s.ListenAndServeTLS("", "")
	}()
	return nil
}
