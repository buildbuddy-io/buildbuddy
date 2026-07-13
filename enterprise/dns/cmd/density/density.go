package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/dns/server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments/gcsflagsync"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs"
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

	dnsPort   = flag.Int("dns.port", 53, "The port to listen for DNS traffic on")
	zoneFiles = flag.Slice[string]("dns.zone_file", []string{}, "Path to a zone file to serve. Repeat the flag to serve records from multiple zones (each file's SOA anchors negative answers for names under its apex). Mutually exclusive with --dns.gcs.bucket.")

	// Zone files can instead be loaded (and watched for changes) from a GCS
	// bucket: every ".zone" object in the bucket is served, each loaded and
	// verified independently. This is mutually exclusive with --dns.zone_file.
	gcsBucket       = flag.String("dns.gcs.bucket", "", "GCS bucket to load and watch zone files (\".zone\" objects) from. Mutually exclusive with --dns.zone_file.")
	gcsCredFile     = flag.String("dns.gcs.credentials_file", "", "Path to a JSON credentials file for the zone-file GCS bucket.")
	gcsCreds        = flag.String("dns.gcs.credentials", "", "JSON credentials for the zone-file GCS bucket.", flag.Secret)
	gcsProject      = flag.String("dns.gcs.project_id", "", "GCP project ID owning the zone-file GCS bucket.")
	gcsPollInterval = flag.Duration("dns.gcs.poll_interval", 30*time.Second, "How often to poll the GCS bucket for zone-file changes.")

	// Self-hosted ACME DNS-01: when dns.acme.gcs.bucket is set, density accepts
	// RFC2136 UPDATEs for _acme-challenge TXT records (authenticated by the TSIG
	// key) and serves them from the GCS bucket, shared across replicas.
	acmeGCSBucket   = flag.String("dns.acme.gcs.bucket", "", "GCS bucket for self-hosted ACME _acme-challenge TXT records. Setting this enables RFC2136 UPDATE handling.")
	acmeGCSCredFile = flag.String("dns.acme.gcs.credentials_file", "", "Path to a JSON credentials file for the ACME GCS bucket.")
	acmeGCSCreds    = flag.String("dns.acme.gcs.credentials", "", "JSON credentials for the ACME GCS bucket.", flag.Secret)
	acmeGCSProject  = flag.String("dns.acme.gcs.project_id", "", "GCP project ID owning the ACME GCS bucket.")
	acmeCacheTTL    = flag.Duration("dns.acme.cache_ttl", 10*time.Second, "How long to cache ACME _acme-challenge TXT lookups before re-reading from GCS.")
	acmeTSIGName    = flag.String("dns.acme.tsig_key_name", "", "TSIG key name authorizing RFC2136 UPDATEs of _acme-challenge records.")
	acmeTSIGSecret  = flag.String("dns.acme.tsig_secret", "", "Base64 TSIG secret for dns.acme.tsig_key_name.", flag.Secret)
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

	// Experiments are sourced in-process from GCS (the DNS server does not run
	// a separate flagd backend). When no GCS bucket is configured, experiments
	// are simply disabled and flags resolve to their defaults.
	if gcsflagsync.Enabled() {
		syncProvider, err := gcsflagsync.New(context.Background())
		if err != nil {
			log.Fatalf("Could not configure experiments GCS sync: %s", err)
		}
		if err := experiments.RegisterInProcessSync(env, syncProvider); err != nil {
			log.Fatalf("Could not register experiments: %s", err)
		}
	}

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
	// Zones come from exactly one source: local files or a GCS bucket.
	gcsEnabled := *gcsBucket != ""
	if gcsEnabled && len(*zoneFiles) > 0 {
		return status.FailedPreconditionError("--dns.gcs.bucket and --dns.zone_file are mutually exclusive; configure exactly one zone source")
	}
	if !gcsEnabled && len(*zoneFiles) == 0 {
		return status.FailedPreconditionError("no zone source configured; set --dns.zone_file or --dns.gcs.bucket")
	}

	// Local zone files are static and can't self-heal, so they're loaded
	// fail-fast up front (each must define an SOA at its apex). GCS zones are
	// loaded, and reloaded, by the watcher below, which is non-fatal and keeps
	// the last-good version of each file.
	var records []dns.RR
	for _, zoneFile := range *zoneFiles {
		rrs, err := server.ParseAndVerifyZoneFile(zoneFile)
		if err != nil {
			return status.WrapErrorf(err, "load zone file %q", zoneFile)
		}
		records = append(records, rrs...)
	}

	// Self-hosted ACME (RFC2136 UPDATE + blobstore-backed challenge store) is
	// enabled by configuring an ACME GCS bucket; the TSIG key authorizes updates.
	var acme *server.Challenges
	tsigSecrets := map[string]string{}
	if *acmeGCSBucket != "" {
		// Without a TSIG key, every RFC2136 UPDATE fails authentication and
		// cert-manager's challenge writes are silently rejected, so refuse to
		// start half-enabled rather than hang issuance with no diagnostic.
		if *acmeTSIGName == "" {
			return status.FailedPreconditionError("dns.acme.gcs.bucket is set but dns.acme.tsig_key_name is empty; RFC2136 UPDATEs could not be authenticated")
		}
		bs, err := gcs.NewGCSBlobStore(context.Background(), *acmeGCSBucket, *acmeGCSCredFile, *acmeGCSCreds, *acmeGCSProject, false /*=enableCompression*/)
		if err != nil {
			return status.WrapError(err, "init ACME challenge blobstore")
		}
		acme, err = server.NewChallenges(bs, *acmeCacheTTL)
		if err != nil {
			return status.WrapError(err, "init ACME challenge store")
		}
		tsigSecrets[dns.Fqdn(*acmeTSIGName)] = *acmeTSIGSecret
	}

	handler := server.NewHandler(env, records, acme)

	// When zones are sourced from GCS, start the watcher: it does an initial
	// best-effort load and then polls for changes, hot-swapping the handler's
	// records. It's non-fatal, so the server comes up even if the bucket is
	// momentarily unreachable or has no valid zone files yet.
	if gcsEnabled {
		if *gcsPollInterval <= 0 {
			return status.InvalidArgumentErrorf("--dns.gcs.poll_interval must be positive, got %s", *gcsPollInterval)
		}
		bs, err := gcs.NewGCSBlobStore(context.Background(), *gcsBucket, *gcsCredFile, *gcsCreds, *gcsProject, false /*=enableCompression*/)
		if err != nil {
			return status.WrapError(err, "init zone-file blobstore")
		}
		watchCtx, cancelWatch := context.WithCancel(context.Background())
		env.GetHealthChecker().RegisterShutdownFunction(func(context.Context) error {
			cancelWatch()
			return nil
		})
		server.WatchZoneFiles(watchCtx, handler, bs, *gcsPollInterval)
	}

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
	udpServer := &dns.Server{PacketConn: packetConn, Handler: handler, TsigSecret: tsigSecrets, MsgAcceptFunc: server.MsgAccept}
	tcpServer := &dns.Server{Listener: listener, Handler: handler, TsigSecret: tsigSecrets, MsgAcceptFunc: server.MsgAccept}

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
