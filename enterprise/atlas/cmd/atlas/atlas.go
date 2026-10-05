package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/app"
	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/atlas_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/cluster"
	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/summaries"
	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/web"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"

	atlaspb "github.com/buildbuddy-io/buildbuddy/proto/atlas"
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	serverType     = flag.String("server_type", "atlas-server", "The server type to match on health checks")
	monitoringAddr = flag.String("monitoring.listen", ":9090", "Address to listen for monitoring traffic on")
	clusterLinks   = flag.Slice("atlas.cluster_links", []clusterLink{}, "Every atlas instance (name and URL), this one included, for the UI's cluster picker.")
)

// clusterLink is one entry of atlas.cluster_links.
type clusterLink struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

func main() {
	version.Print("BuildBuddy Atlas")

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
	env.SetAuthenticator(nullauth.NewNullAuthenticator(true /*=anonymousUsageEnabled*/))
	env.SetListenAddr(*listen)

	ix := summaries.New()
	c, err := cluster.New(ix)
	if err != nil {
		log.Fatalf("Could not set up the cluster: %s", err)
	}
	go c.Run(context.Background())
	service := atlas_service.New(c)

	// The API is served as gRPC, and over HTTP for the UI via protolet, which
	// routes streaming RPCs through the same gRPC server.
	grpcServer, err := grpc_server.New(env, grpc_server.GRPCPort(), false /*=ssl*/, grpc_server.GRPCServerConfig{})
	if err != nil {
		log.Fatalf("Could not create gRPC server: %s", err)
	}
	atlaspb.RegisterAtlasServiceServer(grpcServer.GetServer(), service)
	if err := grpcServer.Start(); err != nil {
		log.Fatalf("Could not start gRPC server: %s", err)
	}

	appFS, err := app.GetAppFS()
	if err != nil {
		log.Fatalf("Could not load the app bundle: %s", err)
	}
	var links []*atlaspb.ClusterLink
	for _, l := range *clusterLinks {
		links = append(links, &atlaspb.ClusterLink{Name: l.Name, Url: l.URL})
	}
	ui, err := web.Handler(env, web.Options{
		AppFS:        appFS,
		Service:      service,
		GRPCServer:   grpcServer.GetServer(),
		ClusterName:  c.Name(),
		ClusterLinks: links,
	})
	if err != nil {
		log.Fatalf("Could not set up the UI: %s", err)
	}

	mux := env.GetMux()
	mux.Handle("/", ui)
	mux.Handle("/healthz", healthChecker.LivenessHandler())
	mux.Handle("/readyz", healthChecker.ReadinessHandler())

	monitoring.StartMonitoringHandler(env, *monitoringAddr)

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", *listen, *port),
		Handler: mux,
	}
	env.GetHTTPServerWaitGroup().Add(1)
	healthChecker.RegisterShutdownFunction(func(ctx context.Context) error {
		defer env.GetHTTPServerWaitGroup().Done()
		return server.Shutdown(ctx)
	})
	go func() {
		log.Infof("Listening on %s", server.Addr)
		_ = server.ListenAndServe()
	}()
	healthChecker.WaitForGracefulShutdown()
}
