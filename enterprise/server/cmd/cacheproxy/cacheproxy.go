package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/cache_proxy"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"google.golang.org/grpc/metadata"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	listenAddr  = flag.String("listen_addr", "localhost:1991", "Local address to listen on.")
	remoteCache = flag.String("remote_cache", "", "Server address to proxy.")
	serverType  = flag.String("server_type", "buildbuddy-proxy", "The server type to match on health checks")

	monitoringAddr = flag.String("monitoring_addr", "localhost:9090", "Local address to listen on.")
)

func registerCacheProxy(ctx context.Context, env *real_environment.RealEnv, grpcServer *grpc.Server) {
	conn, err := grpc_client.DialSimple(*remoteCache)
	if err != nil {
		log.Fatalf("Error dialing remote cache: %s", err.Error())
	}
	cacheProxy, err := cache_proxy.NewCacheProxy(ctx, env, conn)
	if err != nil {
		log.Fatalf("Error initializing cache proxy: %s", err.Error())
	}
	bspb.RegisterByteStreamServer(grpcServer, cacheProxy)
	repb.RegisterActionCacheServer(grpcServer, cacheProxy)
	repb.RegisterContentAddressableStorageServer(grpcServer, cacheProxy)
	repb.RegisterCapabilitiesServer(grpcServer, cacheProxy)
	log.Infof("Cache proxy: will proxy requests to %s", *remoteCache)
}

func propagateAPIKeyFromIncomingToOutgoing(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		keys := md.Get("x-buildbuddy-api-key")
		if len(keys) > 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", keys[len(keys)-1])
		}
	}
	return ctx
}

func propagateAPIKeyUnaryInterceptor() grpc.UnaryServerInterceptor {
	return interceptors.ContextReplacingUnaryServerInterceptor(propagateAPIKeyFromIncomingToOutgoing)
}

func propagateAPIKeyStreamInterceptor() grpc.StreamServerInterceptor {
	return interceptors.ContextReplacingStreamServerInterceptor(propagateAPIKeyFromIncomingToOutgoing)
}

func initializeGRPCServer(env *real_environment.RealEnv) (*grpc.Server, net.Listener) {
	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %s", err.Error())
	}
	log.Debugf("gRPC listening on %q", *listenAddr)
	grpcOptions := []grpc.ServerOption{
		interceptors.GetUnaryInterceptor(env),
		interceptors.GetStreamInterceptor(env),
		grpc.ChainUnaryInterceptor(propagateAPIKeyUnaryInterceptor()),
		grpc.ChainStreamInterceptor(propagateAPIKeyStreamInterceptor()),
		grpc.MaxRecvMsgSize(grpc_server.MaxRecvMsgSizeBytes()),
	}
	grpcServer := grpc.NewServer(grpcOptions...)
	reflection.Register(grpcServer)
	return grpcServer, lis
}

func main() {
	version.Print()

	// Flags must be parsed before config secrets integration is enabled since
	// that feature itself depends on flag values.
	flag.Parse()

	if err := config.Load(); err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}
	config.ReloadOnSIGHUP()

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)
	env.SetAuthenticator(&nullauth.NullAuthenticator{})
	env.SetMux(tracing.NewHttpServeMux(http.NewServeMux()))

	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}

	if err := pebble_cache.Register(env); err != nil {
		log.Fatal(err.Error())
	}

	if c := env.GetCache(); c == nil {
		log.Fatalf("No local cache configured")
	}

	grpcServer, lis := initializeGRPCServer(env)
	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(grpcServer))
	registerCacheProxy(context.Background(), env, grpcServer)

	monitoring.StartMonitoringHandler(env, *monitoringAddr)
	mux := env.GetMux()
	mux.Handle("/healthz", env.GetHealthChecker().LivenessHandler())
	mux.Handle("/readyz", env.GetHealthChecker().ReadinessHandler())

	log.Printf("Listening on %s", lis.Addr())
	grpcServer.Serve(lis)
	env.GetHealthChecker().WaitForGracefulShutdown()
}
