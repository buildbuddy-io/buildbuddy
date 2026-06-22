package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/dns/server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	http_interceptors "github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"

	"github.com/miekg/dns"
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	sslPort        = flag.Int("ssl_port", 8081, "The port to listen for HTTPS traffic on")
	serverType     = flag.String("server_type", "dns-server", "The server type to match on health checks")
	monitoringAddr = flag.String("monitoring.listen", ":9090", "Address to listen for monitoring traffic on")

	dnsPort  = flag.Int("dns.port", 53, "The port to listen for DNS traffic on")
	zoneFile = flag.String("dns.zone_file", "", "Path to a zone file containing the DNS records to serve")
)

func main() {
	version.Print("DeNSity: BuildBuddy DNS Server")

	// Flags must be parsed before config secrets integration is enabled since
	// that feature itself depends on flag values.
	flag.Parse()
	if err := configsecrets.Configure(); err != nil {
		log.Fatalf("Could not prepare config secrets provider: %s", err)
	}
	if err := config.Load(); err != nil {
		log.Fatalf("Could not load config: %s", err)
	}
	config.ReloadOnSIGHUP()

	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)
	if err := tracing.Configure(env); err != nil {
		log.Fatalf("Could not configure tracing: %s", err)
	}
	env.SetMux(tracing.NewHttpServeMux(http.NewServeMux()))

	env.SetListenAddr(*listen)
	if err := ssl.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	if err := startDNSServer(env); err != nil {
		log.Fatalf("%v", err)
	}

	monitoring.StartMonitoringHandler(env, *monitoringAddr)
	env.GetMux().Handle("/healthz", env.GetHealthChecker().LivenessHandler())
	env.GetMux().Handle("/readyz", env.GetHealthChecker().ReadinessHandler())

	httpServer := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", *listen, *port),
		Handler: env.GetMux(),
	}

	env.GetHTTPServerWaitGroup().Add(1)
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		defer env.GetHTTPServerWaitGroup().Done()
		return httpServer.Shutdown(ctx)
	})

	if env.GetSSLService().IsEnabled() {
		tlsConfig, sslHandler := env.GetSSLService().ConfigureTLS(httpServer.Handler)
		sslServer := &http.Server{
			Addr:      fmt.Sprintf("%s:%d", *listen, *sslPort),
			Handler:   httpServer.Handler,
			TLSConfig: tlsConfig,
		}
		go func() {
			log.Debugf("Listening for HTTPS traffic on %s", sslServer.Addr)
			sslServer.ListenAndServeTLS("", "")
		}()
		go func() {
			addr := fmt.Sprintf("%s:%d", *listen, *port)
			log.Debugf("Listening for HTTP traffic on %s", addr)
			http.ListenAndServe(addr, http_interceptors.RedirectIfNotForwardedHTTPS(sslHandler))
		}()
	} else {
		log.Debug("SSL Disabled")
		// If no SSL is enabled, we'll just serve things as-is.
		go func() {
			log.Debugf("Listening for HTTP traffic on %s", httpServer.Addr)
			httpServer.ListenAndServe()
		}()
	}
	env.GetHealthChecker().WaitForGracefulShutdown()
}

func startDNSServer(env *real_environment.RealEnv) error {
	if *zoneFile == "" {
		return status.FailedPreconditionError("a --dns.zone_file must be configured")
	}
	records, err := server.ParseZoneFile(*zoneFile)
	if err != nil {
		return status.WrapErrorf(err, "parse zone file %q", *zoneFile)
	}
	handler := server.NewHandler(records)

	addr := fmt.Sprintf("%s:%d", *listen, *dnsPort)
	// DNS is served over both UDP (the default transport) and TCP (used for
	// responses too large for a single UDP datagram, e.g. zone transfers).
	udpServer := &dns.Server{Addr: addr, Net: "udp", Handler: handler}
	tcpServer := &dns.Server{Addr: addr, Net: "tcp", Handler: handler}

	for _, s := range []*dns.Server{udpServer, tcpServer} {
		s := s
		go func() {
			log.Infof("Listening for DNS (%s) traffic on %s", s.Net, s.Addr)
			if err := s.ListenAndServe(); err != nil {
				log.Errorf("DNS %s server failed: %s", s.Net, err)
			}
		}()
	}

	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		udpErr := udpServer.ShutdownContext(ctx)
		tcpErr := tcpServer.ShutdownContext(ctx)
		if udpErr != nil {
			return udpErr
		}
		return tcpErr
	})

	return nil
}
