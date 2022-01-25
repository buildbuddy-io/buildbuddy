package main

import (
	"context"
	"flag"
	"log"
	"net"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/cache_proxy"
	"github.com/buildbuddy-io/buildbuddy/cli/devnull"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_proxy"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rpcfilters "github.com/buildbuddy-io/buildbuddy/server/rpc/filters"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	serverType = flag.String("server_type", "sidecar", "The server type to match on health checks")

	listenAddr  = flag.String("listen_addr", "localhost:1991", "Local address to listen on.")
	besBackend  = flag.String("bes_backend", "grpcs://cloud.buildbuddy.io:443", "Server address to proxy build events to.")
	remoteCache = flag.String("remote_cache", "", "Server address to cache events to.")

	cacheDir          = flag.String("cache_directory", "", "Root directory to use for local cache")
	cacheMaxSizeBytes = flag.Int64("cache_max_size_bytes", 0, "Max cache size, in bytes")
)

func initializeEnv(configurator *config.Configurator) *real_environment.RealEnv {
	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(configurator, healthChecker)
	env.SetAuthenticator(&nullauth.NullAuthenticator{})
	env.SetBuildEventHandler(&devnull.BuildEventHandler{})
	return env
}

func initializeGRPCServer(env *real_environment.RealEnv) (*grpc.Server, net.Listener) {
	var lis net.Listener
	var err error
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
	return grpcServer, lis
}

func registerBESProxy(env *real_environment.RealEnv, grpcServer *grpc.Server) {
	besTarget := normalizeGrpcTarget(*besBackend)
	buildEventProxyClients := make([]pepb.PublishBuildEventClient, 0)
	buildEventProxyClients = append(buildEventProxyClients, build_event_proxy.NewBuildEventProxyClient(env, besTarget))
	log.Printf("Proxy: forwarding build events to: %q", besTarget)
	env.SetBuildEventProxyClients(buildEventProxyClients)

	// Register to handle build event protocol messages.
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer(env)
	if err != nil {
		log.Fatalf("Error initializing BuildEventProtocolServer: %s", err.Error())
	}
	pepb.RegisterPublishBuildEventServer(grpcServer, buildEventServer)
}

func registerCacheProxy(ctx context.Context, env *real_environment.RealEnv, grpcServer *grpc.Server) {
	cacheTarget := normalizeGrpcTarget(*remoteCache)
	conn, err := grpc_client.DialTarget(cacheTarget)
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
}

func normalizeGrpcTarget(target string) string {
	if strings.HasPrefix(target, "grpc://") || strings.HasPrefix(target, "grpcs://") {
		return target
	}
	return "grpcs://" + target
}

func initializeDiskCache(env *real_environment.RealEnv) {
	maxSizeBytes := int64(1e9) // 1 GB
	if *cacheMaxSizeBytes != 0 {
		maxSizeBytes = *cacheMaxSizeBytes
	}
	c, err := disk_cache.NewDiskCache(env, &config.DiskConfig{RootDirectory: *cacheDir}, maxSizeBytes)
	if err != nil {
		log.Fatalf("Error configuring cache: %s", err)
	}
	env.SetCache(c)
}

func main() {
	flag.Parse()
	configurator, err := config.NewConfigurator("")
	if err != nil {
		log.Fatalf("Error initializing Configurator: %s", err.Error())
	}
	ctx := context.Background()
	env := initializeEnv(configurator)
	grpcServer, lis := initializeGRPCServer(env)
	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(grpcServer))

	if *cacheDir != "" {
		initializeDiskCache(env)
	}
	if *besBackend != "" {
		registerBESProxy(env, grpcServer)
	}
	if *remoteCache != "" {
		registerCacheProxy(ctx, env, grpcServer)
	}
	if *besBackend != "" || *remoteCache != "" {
		grpcServer.Serve(lis)
	} else {
		log.Fatal("No services configured. At least one of --bes_backend or --remote_cache must be provided!")
	}
}
