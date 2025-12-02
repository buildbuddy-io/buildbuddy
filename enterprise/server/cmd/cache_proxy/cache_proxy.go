package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/action_cache_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/atime_updater"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/byte_stream_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/capabilities_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/content_addressable_storage_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/hit_tracker_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_crypter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remoteauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/routing_action_cache_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/routing_byte_stream_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/routing_capabilities_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/routing_content_addressable_storage_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/routing_service"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_asset/fetch_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/scratchspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"google.golang.org/grpc"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	http_interceptors "github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	channelzservice "google.golang.org/grpc/channelz/service"
)

var (
	listen          = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port            = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	sslPort         = flag.Int("ssl_port", 8081, "The port to listen for HTTPS traffic on")
	monitoringPort  = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on")
	httpProxyTarget = flag.String("http_proxy_target", "", "The HTTP target to forward unknown HTTP requests to.")

	serverType = flag.String("server_type", "cache-proxy", "The server type to match on health checks")

	remoteCache = flag.String("cache_proxy.remote_cache", "grpcs://remote.buildbuddy.dev", "The backing remote cache.")

	headersToPropagate = []string{
		authutil.APIKeyHeader,
		authutil.ContextTokenStringKey,
		usageutil.ClientHeaderName,
		usageutil.OriginHeaderName,
		authutil.ClientIdentityHeaderName,
		bazel_request.RequestMetadataKey}
)

func main() {
	version.Print("BuildBuddy cache proxy")

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
		log.Fatalf("%v", err)
	}
	env.SetAuthenticator(authenticator)

	hit_tracker_client.Register(env)
	if err := remote_crypter.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if err := experiments.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	// Configure a local cache.
	if err := pebble_cache.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	if c := env.GetCache(); c == nil {
		log.Fatalf("No local cache configured")
	}
	if err := distributed.Register(env); err != nil {
		log.Fatal(err.Error())
	}
	usageutil.SetServerName("cache-proxy")

	env.SetListenAddr(*listen)
	if err := ssl.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

	if err := startInternalGRPCServers(env); err != nil {
		log.Fatalf("Could not start internal GRPC server: %s", err)
	}

	if err := startGRPCServers(env); err != nil {
		log.Fatalf("%v", err)
	}

	monitoring.StartMonitoringHandler(env, fmt.Sprintf("%s:%d", *listen, *monitoringPort))
	env.GetMux().Handle("/healthz", env.GetHealthChecker().LivenessHandler())
	env.GetMux().Handle("/readyz", env.GetHealthChecker().ReadinessHandler())

	if *httpProxyTarget != "" {
		httpProxyUrl, err := url.Parse(*httpProxyTarget)
		if err != nil {
			log.Fatalf("Could not parse http proxy url: %v", err)
		}
		proxy := httputil.NewSingleHostReverseProxy(httpProxyUrl)

		// Fallback to forward any unmatched HTTP routes to the proxy target.
		env.GetMux().Handle("/", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			proxy.ServeHTTP(w, req)
		}))
	}

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
	// Add the API-Key, JWT, client-identity, etc... propagating interceptor.
	grpcServerConfig := grpc_server.GRPCServerConfig{
		ExtraChainedUnaryInterceptors: []grpc.UnaryServerInterceptor{
			interceptors.PropagateMetadataUnaryInterceptor(headersToPropagate...),
		},
		ExtraChainedStreamInterceptors: []grpc.StreamServerInterceptor{
			interceptors.PropagateMetadataStreamInterceptor(headersToPropagate...),
		},
	}

	// Start and register internal servers first because the external proxy
	// services rely on the internal servers.
	if err := registerInternalServices(env); err != nil {
		return err
	}

	s, err := grpc_server.New(env, grpc_server.GRPCPort(), false, grpcServerConfig)
	if err != nil {
		return err
	}
	registerGRPCServices(s.GetServer(), env)
	if err := s.Start(); err != nil {
		return err
	}

	// Register local clients pointing to the proxy servers.
	conn, err := grpc_client.DialInternal(env, fmt.Sprintf("grpc://localhost:%d", grpc_server.GRPCPort()))
	if err != nil {
		return status.InternalErrorf("Error dialing local gRPC server: %s", err.Error())
	}
	env.SetLocalByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetLocalContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))

	if env.GetSSLService().IsEnabled() {
		s, err = grpc_server.New(env, grpc_server.GRPCSPort(), true, grpcServerConfig)
		if err != nil {
			return err
		}
		registerGRPCServices(s.GetServer(), env)
		if err := s.Start(); err != nil {
			return err
		}
	}
	return nil
}

func startInternalGRPCServers(env *real_environment.RealEnv) error {
	b, err := grpc_server.New(env, grpc_server.InternalGRPCPort(), false /*=ssl*/, grpc_server.GRPCServerConfig{})
	if err != nil {
		return err
	}
	channelzservice.RegisterChannelzServiceToServer(b.GetServer())
	if err = b.Start(); err != nil {
		return err
	}
	env.SetInternalGRPCServer(b.GetServer())
	return nil
}

func registerGRPCServices(grpcServer *grpc.Server, env *real_environment.RealEnv) {
	// Connect to the remote cache and initialize gRPC clients.
	conn, err := grpc_client.DialInternal(env, *remoteCache)
	if err != nil {
		log.Fatalf("Error dialing remote cache: %s", err.Error())
	}
	// TODO: Before this can be enabled, https://github.com/buildbuddy-io/buildbuddy-internal/issues/6146
	// must be resolved.
	if routing_service.IsCacheRoutingEnabled() {
		if err := routing_service.RegisterRoutingService(env); err != nil {
			log.Fatalf("Error initializing routing service: %s", err.Error())
		}

		ac, err := routing_action_cache_client.New(env)
		if err != nil {
			log.Fatalf("Error initializing routing action cache client: %s", err.Error())
		}
		env.SetActionCacheClient(ac)

		cap, err := routing_capabilities_client.New(env)
		if err != nil {
			log.Fatalf("Error initializing routing capabilities client: %s", err.Error())
		}
		env.SetCapabilitiesClient(cap)

		bs, err := routing_byte_stream_client.New(env)
		if err != nil {
			log.Fatalf("Error initializing routing bytestream client: %s", err.Error())
		}
		env.SetByteStreamClient(bs)

		cas, err := routing_content_addressable_storage_client.New(env)
		if err != nil {
			log.Fatalf("Error initializing routing CAS client: %s", err.Error())
		}
		env.SetContentAddressableStorageClient(cas)
	} else {
		env.SetActionCacheClient(repb.NewActionCacheClient(conn))
		env.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))
		env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
		env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
		env.SetBuildBuddyServiceClient(bbspb.NewBuildBuddyServiceClient(conn))
	}

	// The atime updater must be registered after the remote CAS client (which
	// it depends on), but before the local CAS server (which depends on it).
	if err := atime_updater.Register(env); err != nil {
		log.Fatalf("%v", err)
	}

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
	if err := scratchspace.Init(); err != nil {
		log.Fatalf("Failed to initialize temp storage directory: %s", err)
	}
	if err := fetch_server.Register(env); err != nil {
		log.Fatalf("%v", err)
	}
	repb.RegisterActionCacheServer(grpcServer, env.GetActionCacheServer())
	bspb.RegisterByteStreamServer(grpcServer, env.GetByteStreamServer())
	repb.RegisterContentAddressableStorageServer(grpcServer, env.GetCASServer())
	repb.RegisterCapabilitiesServer(grpcServer, env.GetCapabilitiesServer())
	rapb.RegisterFetchServer(grpcServer, env.GetFetchServer())
	log.Infof("Cache proxy proxying requests to %s", *remoteCache)
}

func registerInternalServices(env *real_environment.RealEnv) error {
	localBSS, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		return status.InternalErrorf("CacheProxy: error starting local bytestream server: %s", err.Error())
	}
	env.SetLocalByteStreamServer(localBSS)

	localCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		return status.InternalErrorf("CacheProxy: error starting local contentaddressablestorage server: %s", err.Error())
	}
	env.SetLocalCASServer(localCAS)

	localAC, err := action_cache_server.NewActionCacheServer(env)
	if err != nil {
		return status.InternalErrorf("CacheProxy: error starting local actioncache server: %s", err.Error())
	}
	env.SetLocalActionCacheServer(localAC)
	return nil
}
