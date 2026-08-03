// The execution_graph_worker binary polls for completed invocations and
// analyzes their execution graph logs. See the execution_graph_worker
// package for details.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_graph_worker"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"

	_ "github.com/buildbuddy-io/buildbuddy/server/util/kuberesolver" // registers kube:// resolver.
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	monitoringPort = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")
	serverType     = flag.String("server_type", "execution-graph-worker", "The server type to match on health checks")
)

func main() {
	version.Print("BuildBuddy Execution Graph Worker")

	// Flags must be parsed before config secrets integration is enabled since
	// that feature itself depends on flag values.
	flag.Parse()
	if err := configsecrets.Configure(); err != nil {
		log.Fatalf("Could not prepare config secrets provider: %s", err)
	}
	if err := config.Load(); err != nil {
		log.Fatalf("Error loading config from file: %s", err)
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

	if err := blobstore.Register(env); err != nil {
		log.Fatalf("Could not configure blobstore: %s", err)
	}
	if err := clickhouse.Register(env); err != nil {
		log.Fatalf("Could not configure ClickHouse: %s", err)
	}

	// Used as the fallback when the graph log wasn't persisted to blobstore;
	// dials the cache named by each bytestream URI directly.
	byte_stream_client.RegisterPooledBytestreamClient(env)

	worker, err := execution_graph_worker.New(env)
	if err != nil {
		log.Fatalf("Could not create execution graph worker: %s", err)
	}
	ctx, cancel := context.WithCancel(env.GetServerContext())
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		cancel()
		return nil
	})
	go worker.Start(ctx)

	monitoring.StartMonitoringHandler(env, fmt.Sprintf("%s:%d", *listen, *monitoringPort))
	env.GetMux().Handle("/healthz", env.GetHealthChecker().LivenessHandler())
	env.GetMux().Handle("/readyz", env.GetHealthChecker().ReadinessHandler())

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", *listen, *port),
		Handler: env.GetMux(),
	}
	env.GetHTTPServerWaitGroup().Add(1)
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		defer env.GetHTTPServerWaitGroup().Done()
		return server.Shutdown(ctx)
	})
	go func() {
		log.Debugf("Listening for HTTP traffic on %s", server.Addr)
		server.ListenAndServe()
	}()

	env.GetHealthChecker().WaitForGracefulShutdown()
}
