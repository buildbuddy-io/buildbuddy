package main

import (
	"fmt"
	"net"
	"os"

	"github.com/buildbuddy-io/buildbuddy/codesearch/server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remoteauth"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"

	"google.golang.org/grpc"

	csspb "github.com/buildbuddy-io/buildbuddy/proto/codesearch_service"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	serverType = flag.String("server_type", "codesearch-server", "The server type to match on health checks")

	listen       = flag.String("codesearch.listen", ":2633", "Address to listen on")
	csIndexDir   = flag.String("codesearch.index_dir", "", "Directory to store index in")
	csScratchDir = flag.String("codesearch.scratch_dir", "", "Directory to store temp files in")
	remoteCache  = flag.String("codesearch.remote_cache", "", "gRPC Address of buildbuddy cache")

	monitoringAddr = flag.String("monitoring.listen", ":9090", "Address to listen for monitoring traffic on")

	headersToPropagate = []string{authutil.APIKeyHeader, authutil.ContextTokenStringKey}
)

func main() {
	flag.Parse()

	if err := configsecrets.Configure(); err != nil {
		log.Fatalf("Could not prepare config secrets provider: %s", err)
	}

	if err := config.Load(); err == nil {
		config.ReloadOnSIGHUP()
	}

	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}

	log.Printf("csIndexDir %s, csScratchDir: %s", *csIndexDir, *csScratchDir)
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err.Error())
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)

	authenticator, err := remoteauth.NewRemoteAuthenticator()
	if err != nil {
		log.Fatal(err.Error())
	}
	env.SetAuthenticator(authenticator)

	monitoring.StartMonitoringHandler(env, *monitoringAddr)

	css, err := server.New(env, *csIndexDir, *csScratchDir)
	if err != nil {
		log.Fatal(err.Error())
	}

	conn, err := grpc_client.DialInternal(env, *remoteCache)
	if err != nil {
		log.Fatal(err.Error())
	}
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))

	// Add the API-Key and JWT propagating interceptors.
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
	csspb.RegisterCodesearchServiceServer(server, css)

	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(server))
	go func() {
		_ = server.Serve(lis)
	}()
	log.Printf("Codesearch server listening at %v", lis.Addr())
	env.GetHealthChecker().WaitForGracefulShutdown()
}
