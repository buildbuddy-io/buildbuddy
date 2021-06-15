package monitoring

import (
	"net/http"
	"net/http/pprof"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registers monitoring handlers on the provided mux. Note that using
// StartMonitoringHandler on a monitoring-only port is preferred.
func RegisterMonitoringHandlers(mux *http.ServeMux) {
	// Prometheus metrics
	mux.Handle("/metrics", promhttp.Handler())

	// PProf endpoints
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Statusz page
	mux.Handle("/statusz", statusz.Server())
}

// StartMonitoringHandler enables the prometheus and pprof monitoring handlers
// in the same mux on the specified host and port, which should not have
// anything else running on it.
func StartMonitoringHandler(hostPort string) {
	mux := http.NewServeMux()
	RegisterMonitoringHandlers(mux)
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
