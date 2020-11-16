package main

import (
	"context"
	"flag"
	"log"
	"net"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_proxy"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	rpcfilters "github.com/buildbuddy-io/buildbuddy/server/rpc/filters"
)

var (
	serverType = flag.String("server_type", "sidecar", "The server type to match on health checks")

	listenAddr = flag.String("listen_addr", "localhost:1991", "Local address to listen on.")
	serverAddr = flag.String("server_addr", "grpcs://cloud.buildbuddy.io:443", "Server address to proxy events to.")
)

type DevNullChannel struct{}

func (c *DevNullChannel) MarkInvocationDisconnected(ctx context.Context, iid string) error {
	return nil
}
func (c *DevNullChannel) FinalizeInvocation(ctx context.Context, iid string) error { return nil }
func (c *DevNullChannel) HandleEvent(ctx context.Context, event *pepb.PublishBuildToolEventStreamRequest) error {
	return nil
}

type DevNullBuildEventHandler struct{}

func (h *DevNullBuildEventHandler) OpenChannel(ctx context.Context, iid string) interfaces.BuildEventChannel {
	return &DevNullChannel{}
}

func main() {
	flag.Parse()
	configurator, err := config.NewConfigurator("")
	if err != nil {
		log.Fatalf("Error initializing Configurator: %s", err.Error())
	}
	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(configurator, healthChecker)
	env.SetAuthenticator(&nullauth.NullAuthenticator{})
	env.SetBuildEventHandler(&DevNullBuildEventHandler{})

	var lis net.Listener
	if strings.HasPrefix(*listenAddr, "unix://") {
		sockPath := strings.TrimPrefix(*listenAddr, "unix://")
		lis, err = net.Listen("unix", sockPath)
	} else {
		lis, err = net.Listen("tcp", *listenAddr)
	}
	if err != nil {
		log.Fatalf("Failed to listen: %s", err.Error())
	}
	log.Printf("gRPC listening on %q", *listenAddr)
	grpcOptions := []grpc.ServerOption{
		rpcfilters.GetUnaryInterceptor(env),
		rpcfilters.GetStreamInterceptor(env),
		grpc.MaxRecvMsgSize(env.GetConfigurator().GetGRPCMaxRecvMsgSizeBytes()),
	}
	grpcServer := grpc.NewServer(grpcOptions...)
	reflection.Register(grpcServer)
	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(grpcServer))

	buildEventProxyClients := make([]pepb.PublishBuildEventClient, 0)
	buildEventProxyClients = append(buildEventProxyClients, build_event_proxy.NewBuildEventProxyClient(*serverAddr))
	log.Printf("Proxy: forwarding build events to: %q", *serverAddr)
	env.SetBuildEventProxyClients(buildEventProxyClients)

	// Register to handle build event protocol messages.
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer(env)
	if err != nil {
		log.Fatalf("Error initializing BuildEventProtocolServer: %s", err.Error())
	}
	pepb.RegisterPublishBuildEventServer(grpcServer, buildEventServer)

	grpcServer.Serve(lis)
}
