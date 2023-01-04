// TODO: Move this out of `cmd` since it is no longer a cmd.
package sidecar

import (
	"context"
	"flag"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/cache_proxy"
	"github.com/buildbuddy-io/buildbuddy/cli/devnull"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_proxy"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/sidecar"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// Default max size for the sidecar's local cache.
	defaultMaxCacheSizeBytes = 10_000_000_000 // 10 GB
)

var (
	serverType = flag.String("server_type", "sidecar", "The server type to match on health checks")

	listenAddr  = flag.String("listen_addr", "localhost:1991", "Local address to listen on.")
	besBackend  = flag.String("bes_backend", "", "Server address to proxy build events to.")
	remoteCache = flag.String("remote_cache", "", "Server address to cache events to.")

	cacheDir          = flag.String("cache_dir", "", "Root directory to use for local cache")
	cacheMaxSizeBytes = flag.Int64("cache_max_size_bytes", 0, "Max cache size, in bytes")
	inactivityTimeout = flag.Duration("inactivity_timeout", 5*time.Minute, "Sidecar will terminate after this much inactivity")
)

var (
	lastUseMu sync.RWMutex
	lastUse   time.Time
)

func maybeUpdateLastUse() {
	lastUseMu.RLock()
	needsUpdate := time.Since(lastUse) > time.Second
	lastUseMu.RUnlock()

	if !needsUpdate {
		return
	}

	lastUseMu.Lock()
	lastUse = time.Now()
	lastUseMu.Unlock()
}

func startInactivityWatcher(ctx context.Context, inactiveCallbackFn func()) {
	maybeUpdateLastUse()
	go func() {
		active := true
		for active {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				lastUseMu.RLock()
				if time.Since(lastUse) > *inactivityTimeout {
					active = false
				}
				lastUseMu.RUnlock()
			}
		}
		inactiveCallbackFn()
	}()
}

func inactivityUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		maybeUpdateLastUse()
		return handler(ctx, req)
	}
}

func inactivityStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		maybeUpdateLastUse()
		return handler(srv, stream)
	}
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

func initializeEnv() *real_environment.RealEnv {
	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)
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
	log.Debugf("gRPC listening on %q", *listenAddr)
	grpcOptions := []grpc.ServerOption{
		interceptors.GetUnaryInterceptor(env),
		interceptors.GetStreamInterceptor(env),
		grpc.ChainUnaryInterceptor(inactivityUnaryInterceptor(), propagateAPIKeyUnaryInterceptor()),
		grpc.ChainStreamInterceptor(inactivityStreamInterceptor(), propagateAPIKeyStreamInterceptor()),
		grpc.MaxRecvMsgSize(grpc_server.MaxRecvMsgSizeBytes()),
	}
	grpcServer := grpc.NewServer(grpcOptions...)
	reflection.Register(grpcServer)
	return grpcServer, lis
}

func registerBESProxy(env *real_environment.RealEnv, grpcServer *grpc.Server) {
	besTarget := normalizeGrpcTarget(*besBackend)
	buildEventProxyClients := make([]pepb.PublishBuildEventClient, 0)
	buildEventProxyClients = append(buildEventProxyClients, build_event_proxy.NewBuildEventProxyClient(env, besTarget))
	env.SetBuildEventProxyClients(buildEventProxyClients)

	// Register to handle build event protocol messages.
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer(env)
	if err != nil {
		log.Fatalf("Error initializing BuildEventProtocolServer: %s", err.Error())
	}
	pepb.RegisterPublishBuildEventServer(grpcServer, buildEventServer)
	log.Infof("BES proxy: will proxy requests to %q", besTarget)
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
	log.Infof("Cache proxy: will proxy requests to %s", cacheTarget)
}

type sidecarService struct{}

func (s *sidecarService) Ping(ctx context.Context, req *scpb.PingRequest) (*scpb.PingResponse, error) {
	return &scpb.PingResponse{}, nil
}

func normalizeGrpcTarget(target string) string {
	if strings.HasPrefix(target, "grpc://") || strings.HasPrefix(target, "grpcs://") {
		return target
	}
	return "grpcs://" + target
}

func initializeDiskCache(env *real_environment.RealEnv) {
	maxSizeBytes := int64(defaultMaxCacheSizeBytes)
	if *cacheMaxSizeBytes != 0 {
		maxSizeBytes = *cacheMaxSizeBytes
	}
	c, err := disk_cache.NewDiskCache(env, &disk_cache.Options{RootDirectory: *cacheDir}, maxSizeBytes)
	if err != nil {
		log.Fatalf("Error configuring cache: %s", err)
	}
	env.SetCache(c)
}

func Handle() {
	sc, args := arg.Pop(os.Args, "sidecar")
	if sc != "1" {
		return
	}
	defer os.Exit(0)
	os.Args = args

	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := log.Configure(); err != nil {
		log.Fatalf("Failed to configure logging: %s", err)
	}

	env := initializeEnv()
	grpcServer, lis := initializeGRPCServer(env)
	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(grpcServer))

	// Shutdown the server gracefully after a period of inactivity configurable
	// with the --inactivity_timeout flag.
	startInactivityWatcher(ctx, func() {
		env.GetHealthChecker().Shutdown()
	})

	if *cacheDir != "" {
		initializeDiskCache(env)
	}
	if *besBackend != "" {
		registerBESProxy(env, grpcServer)
	}
	if *remoteCache != "" {
		registerCacheProxy(ctx, env, grpcServer)
	}
	if *besBackend == "" && *remoteCache == "" {
		log.Fatal("No services configured. At least one of --bes_backend or --remote_cache must be provided!")
	}

	scpb.RegisterSidecarServer(grpcServer, &sidecarService{})

	log.Printf("Listening on %s", lis.Addr())
	grpcServer.Serve(lis)
	env.GetHealthChecker().WaitForGracefulShutdown()
}
