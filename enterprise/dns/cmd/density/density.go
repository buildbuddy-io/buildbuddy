package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/dns/server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
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
	serverType     = flag.String("server_type", "dns-server", "The server type to match on health checks")
	monitoringAddr = flag.String("monitoring.listen", ":9090", "Address to listen for monitoring traffic on")

	dnsPort    = flag.Int("dns.port", 53, "The port to listen for DNS traffic on")
	zoneFile   = flag.String("dns.zone_file", "", "Path to a zone file containing the DNS records to serve")
	zoneOrigin = flag.String("dns.zone_origin", "", "Origin domain to qualify relative names in the zone file against. If empty, names must be fully qualified.")
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

	if err := startDNSServer(env); err != nil {
		log.Fatalf("%v", err)
	}

	monitoring.StartMonitoringHandler(env, *monitoringAddr)
	env.GetMux().Handle("/healthz", env.GetHealthChecker().LivenessHandler())
	env.GetMux().Handle("/readyz", env.GetHealthChecker().ReadinessHandler())

	// The HTTP server exists only to serve the /healthz and /readyz probes (and
	// is plain HTTP: orchestration health checks don't need TLS).
	httpServer := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", *listen, *port),
		Handler: env.GetMux(),
	}
	env.GetHTTPServerWaitGroup().Add(1)
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		defer env.GetHTTPServerWaitGroup().Done()
		return httpServer.Shutdown(ctx)
	})
	go func() {
		log.Debugf("Listening for HTTP traffic on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log so this doesn't hang if it fails in the goroutine.
			log.Errorf("HTTP probe server failed: %s", err)
		}
	}()

	env.GetHealthChecker().WaitForGracefulShutdown()
}

func startDNSServer(env *real_environment.RealEnv) error {
	if *zoneFile == "" {
		return status.FailedPreconditionError("a --dns.zone_file must be configured")
	}
	records, err := server.ParseZoneFile(*zoneFile, *zoneOrigin)
	if err != nil {
		return status.WrapErrorf(err, "parse zone file %q", *zoneFile)
	}
	handler := server.NewHandler(records)

	addr := fmt.Sprintf("%s:%d", *listen, *dnsPort)

	// DNS is served over both UDP (the default transport) and TCP (used for
	// responses too large for a single UDP datagram). Bind both up front
	// and return an error to fail early if either cannot bind.
	packetConn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return status.WrapErrorf(err, "bind DNS udp %s", addr)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		packetConn.Close()
		return status.WrapErrorf(err, "bind DNS tcp %s", addr)
	}
	udpServer := &dns.Server{PacketConn: packetConn, Handler: handler}
	tcpServer := &dns.Server{Listener: listener, Handler: handler}

	for _, s := range []*dns.Server{udpServer, tcpServer} {
		go func() {
			log.Infof("Listening for DNS traffic on %s", addr)
			// The sockets are already bound; ActivateAndServe only returns on
			// shutdown or an unexpected serving error, which is fatal.
			if err := s.ActivateAndServe(); err != nil {
				log.Fatalf("DNS server failed: %s", err)
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
