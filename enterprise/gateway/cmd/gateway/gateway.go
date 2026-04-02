package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remoteauth"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	http_interceptors "github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"

	"google.golang.org/grpc"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway_service"
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	sslPort        = flag.Int("ssl_port", 8081, "The port to listen for HTTPS traffic on")
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
	env.SetMux(tracing.NewHttpServeMux(http.NewServeMux()))
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

func startGRPCServers(env *real_environment.RealEnv) error {
	gw, err := server.New(env)
	if err != nil {
		return err
	}
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		gw.Close()
		return nil
	})

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

	gwpb.RegisterGatewayServiceServer(s.GetServer(), gw)
	return s.Start()
}
