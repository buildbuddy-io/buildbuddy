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

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
)

var (
	serverType     = flag.String("server_type", "gateway-server", "The server type to match on health checks")
	grpcListen     = flag.String("gateway.listen", ":9473", "Address to listen on (gRPC)")
	monitoringAddr = flag.String("monitoring.listen", ":9090", "Address to listen for monitoring traffic on")

	headersToPropagate = []string{authutil.APIKeyHeader, authutil.ContextTokenStringKey}
)

func main() {
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

	lis, err := net.Listen("tcp", *grpcListen)
	if err != nil {
		log.Fatal(err.Error())
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)

	if err := remoteauth.Register(env); err != nil {
		log.Fatal(err.Error())
	}

	monitoring.StartMonitoringHandler(env, *monitoringAddr)

	gw, err := server.New(env)
	if err != nil {
		log.Fatal(err.Error())
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
		log.Fatal(err.Error())
	}

	server := s.GetServer()
	gwpb.RegisterGatewayServiceServer(server, gw)

	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(server))
	go func() {
		_ = server.Serve(lis)
	}()
	log.Printf("Gateway server listening on %v", lis.Addr())
	env.GetHealthChecker().WaitForGracefulShutdown()
}
