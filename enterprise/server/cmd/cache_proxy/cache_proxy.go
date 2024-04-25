package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/action_cache_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/byte_stream_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/capabilities_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/content_addressable_storage_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	http_interceptors "github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	sslPort        = flag.Int("ssl_port", 8081, "The port to listen for HTTPS traffic on")
	monitoringPort = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")

	serverType = flag.String("server_type", "cache-proxy", "The server type to match on health checks")

	remoteCache = flag.String("cache_proxy.remote_cache", "grpcs://remote.buildbuddy.dev", "The backing remote cache.")
)

func main() {
	version.Print()

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
	env.SetMux(tracing.NewHttpServeMux(http.NewServeMux()))
	env.SetAuthenticator(&nullauth.NullAuthenticator{})

	// Configure a local cache.
	if err := pebble_cache.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if c := env.GetCache(); c == nil {
		log.Fatalf("No local cache configured")
	}

	env.SetListenAddr(*listen)
	if err := ssl.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	if err := startGRPCServers(env); err != nil {
		log.Fatalf("%v", err)
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

func startGRPCServers(env *real_environment.RealEnv) error {
	// Add the API-Key propagating interceptors.
	// TODO(iain): improve auth story so we can run beyond dev.
	grpcServerConfig := grpc_server.GRPCServerConfig{
		ExtraChainedUnaryInterceptors:  []grpc.UnaryServerInterceptor{interceptors.PropagateAPIKeyUnaryInterceptor()},
		ExtraChainedStreamInterceptors: []grpc.StreamServerInterceptor{interceptors.PropagateAPIKeyStreamInterceptor()},
	}

	s, err := grpc_server.New(env, grpc_server.GRPCPort(), false, grpcServerConfig)
	if err != nil {
		return err
	}
	registerGRPCServices(s.GetServer(), env)
	if err := s.Start(); err != nil {
		return err
	}

	s, err = grpc_server.New(env, grpc_server.GRPCSPort(), true, grpcServerConfig)
	if err != nil {
		return err
	}
	registerGRPCServices(s.GetServer(), env)
	if err := s.Start(); err != nil {
		return err
	}
	return nil
}

func registerGRPCServices(grpcServer *grpc.Server, env *real_environment.RealEnv) {
	// Connect to the remote cache and initialize gRPC clients.
	conn, err := grpc_client.DialSimple(*remoteCache)
	if err != nil {
		log.Fatalf("Error dialing remote cache: %s", err.Error())
	}
	env.SetActionCacheClient(repb.NewActionCacheClient(conn))
	env.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))

	// Configure gRPC services.
	if err := capabilities_server_proxy.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if err := action_cache_server_proxy.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if err := byte_stream_server_proxy.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if err := content_addressable_storage_server_proxy.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	repb.RegisterActionCacheServer(grpcServer, env.GetActionCacheServer())
	bspb.RegisterByteStreamServer(grpcServer, env.GetByteStreamServer())
	repb.RegisterContentAddressableStorageServer(grpcServer, env.GetCASServer())
	repb.RegisterCapabilitiesServer(grpcServer, env.GetCapabilitiesServer())
	log.Infof("Cache proxy proxying requests to %s", *remoteCache)
}
