package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/metadata"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remoteauth"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"

	mdspb "github.com/buildbuddy-io/buildbuddy/proto/metadata_service"
	http_interceptors "github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	sslPort        = flag.Int("ssl_port", 8081, "The port to listen for HTTPS traffic on")
	monitoringPort = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")
	serverType     = flag.String("server_type", "metadata-server", "The server type to match on health checks")
)

func main() {
	version.Print("BuildBuddy Metadata Server")

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
	authenticator, err := remoteauth.NewRemoteAuthenticator()
	if err != nil {
		log.Fatal(err.Error())
	}
	env.SetAuthenticator(authenticator)

	env.SetListenAddr(*listen)
	if err := ssl.Register(env); err != nil {
		log.Fatal(err.Error())
	}

	if err := gossip.Register(env); err != nil {
		log.Fatal(err.Error())
	}

	metadataServer, err := metadata.NewFromFlags(env)
	if err != nil {
		log.Fatal(err.Error())
	}
	statusz.AddSection("metadata-server", "Metadata Server", metadataServer)
	env.GetHealthChecker().RegisterShutdownFunction(
		func(ctx context.Context) error {
			metadataServer.Stop(ctx)
			return nil
		},
	)
	env.GetHealthChecker().AddHealthCheck("metadata-server", metadataServer)

	if err := startGRPCServers(env, metadataServer); err != nil {
		log.Fatal(err.Error())
	}

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
		err := server.Shutdown(ctx)
		return err
	})

	if env.GetSSLService().IsEnabled() {
		tlsConfig, sslHandler := env.GetSSLService().ConfigureTLS(server.Handler)
		sslServer := &http.Server{
			Addr:      fmt.Sprintf("%s:%d", *listen, *sslPort),
			Handler:   server.Handler,
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
			log.Debugf("Listening for HTTP traffic on %s", server.Addr)
			server.ListenAndServe()
		}()
	}

	env.GetHealthChecker().WaitForGracefulShutdown()
}

func startGRPCServers(env *real_environment.RealEnv, metadataServer *metadata.Server) error {
	grpcServerConfig := grpc_server.GRPCServerConfig{}

	// Start and register services on the internal gRPC server first because
	// the external proxy services rely on the internal services.
	internalServer, err := grpc_server.New(env, grpc_server.InternalGRPCPort(), false, grpcServerConfig)
	if err != nil {
		return err
	}

	mdspb.RegisterMetadataServiceServer(internalServer.GetServer(), metadataServer)
	if err := internalServer.Start(); err != nil {
		return err
	}

	externalServer, err := grpc_server.New(env, grpc_server.GRPCPort(), false, grpcServerConfig)
	if err != nil {
		return err
	}

	mdspb.RegisterMetadataServiceServer(externalServer.GetServer(), metadataServer)
	if err := externalServer.Start(); err != nil {
		return err
	}

	if env.GetSSLService().IsEnabled() {
		externalServer, err = grpc_server.New(env, grpc_server.GRPCSPort(), true, grpcServerConfig)
		if err != nil {
			return err
		}
		mdspb.RegisterMetadataServiceServer(externalServer.GetServer(), metadataServer)
		if err := externalServer.Start(); err != nil {
			return err
		}
	}
	return nil
}
