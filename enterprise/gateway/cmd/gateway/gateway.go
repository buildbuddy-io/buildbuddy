package main

import (
	"fmt"
	"net"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remoteauth"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"

	"google.golang.org/grpc"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway_service"
)

var (
        listen          = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
        port            = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
        sslPort         = flag.Int("ssl_port", 8081, "The port to listen for HTTPS traffic on")
	serverType     = flag.String("server_type", "gateway-server", "The server type to match on health checks")
	monitoringAddr = flag.String("monitoring.listen", ":9090", "Address to listen for monitoring traffic on")

	headersToPropagate = []string{authutil.APIKeyHeader, authutil.ContextTokenStringKey}
)

func main() {
	version.Print("BuildBuddy Gateway")

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
	if err := remoteauth.Register(env); err != nil {
		log.Fatal(err.Error())
	}

	
        env.SetListenAddr(*listen)
        if err := ssl.Register(env); err != nil {
                log.Fatalf("%v", err)
        }

        if err := startGRPCServers(env); err != nil {
                log.Fatalf("%v", err)
        }
	
	monitoring.StartMonitoringHandler(env, *monitoringAddr)
        env.GetMux().Handle("/healthz", env.GetHealthChecker().LivenessHandler())
        env.GetMux().Handle("/readyz", env.GetHealthChecker().ReadinessHandler())


	if env.GetSSLService().IsEnabled() {
                tlsConfig, sslHandler := env.GetSSLService().ConfigureTLS(server.Handler)
                sslServer := &http.Server{
                        Addr:      fmt.Sprintf("%s:%d", *listen, *sslPort),
                        Handler:   server.Handler,
                        TLSConfig: tlsConfig,
                }
                sslListener, err := net.Listen("tcp", sslServer.Addr)
                if err != nil {
                        log.Fatalf("Failed to listen on SSL port %d: %s", *sslPort, err)
                }
                go func() {
                        log.Debugf("Listening for HTTPS traffic on %s", sslServer.Addr)
                        _ = sslServer.ServeTLS(sslListener, "", "")
                }()
                httpListener, err := net.Listen("tcp", server.Addr)
                if err != nil {
                        log.Fatalf("Failed to listen on port %d: %s", *port, err)
                }
                go func() {
                        log.Debugf("Listening for HTTP traffic on %s", server.Addr)
                        _ = http.Serve(httpListener, http_interceptors.RedirectIfNotForwardedHTTPS(sslHandler))
                }()
        } else {
                log.Debug("SSL Disabled")
                // If no SSL is enabled, we'll just serve things as-is.
                lis, err := net.Listen("tcp", server.Addr)
                if err != nil {
                        log.Fatalf("Failed to listen on port %d: %s", *port, err)
                }
                go func() {
                        log.Debugf("Listening for HTTP traffic on %s", server.Addr)
                        _ = server.Serve(lis)
                }()
        }
	env.GetHealthChecker().WaitForGracefulShutdown()
}

func startGRPCServers(env *real_environment.RealEnv) error {
	gw, err := server.New(env)
	if err != nil {
		return err
	}

	grpcServerConfig := grpc_server.GRPCServerConfig{
		ExtraChainedUnaryInterceptors: []grpc.UnaryServerInterceptor{
			interceptors.PropagateMetadataUnaryInterceptor(headersToPropagate...),
		},
		ExtraChainedStreamInterceptors: []grpc.StreamServerInterceptor{
			interceptors.PropagateMetadataStreamInterceptor(headersToPropagate...),
		},
	}

	s, err := grpc_server.New(env, grpc_server.GRPCPort(), false, grpcServerConfig)
	if err != nil {
		return err
	}

	server := s.GetServer()
	gwpb.RegisterGatewayServiceServer(server, gw)

	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(server))
	go func() {
		_ = server.Serve(lis)
	}()
	log.Printf("Gateway server listening on %v", lis.Addr())

}
