package main

import (
	"net"

	"github.com/buildbuddy-io/buildbuddy/codesearch/server"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	csspb "github.com/buildbuddy-io/buildbuddy/proto/codesearch_service"
	ksspb "github.com/buildbuddy-io/buildbuddy/proto/kythe_service"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	serverType = flag.String("server_type", "codesearch-server", "The server type to match on health checks")

	listen       = flag.String("codesearch.listen", ":2633", "Address to listen on")
	csIndexDir   = flag.String("codesearch.index_dir", "", "Directory to store index in")
	csScratchDir = flag.String("codesearch.scratch_dir", "", "Directory to store temp files in")
	appTarget    = flag.String("codesearch.app_target", "", "gRPC Address of buildbuddy cache")

	monitoringAddr = flag.String("monitoring.listen", ":9090", "Address to listen for monitoring traffic on")
)

func main() {
	flag.Parse()

	if err := config.Load(); err == nil {
		config.ReloadOnSIGHUP()
	}

	log.Printf("csIndexDir %s, csScratchDir: %s", *csIndexDir, *csScratchDir)
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err.Error())
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)
	env.SetAuthenticator(nullauth.NewNullAuthenticator(true /*anonymousEnabled*/, ""))

	monitoring.StartMonitoringHandler(env, *monitoringAddr)

	css, err := server.New(env, *csIndexDir, *csScratchDir)
	if err != nil {
		log.Fatal(err.Error())
	}

	conn, err := grpc_client.DialInternal(env, *appTarget)
	if err != nil {
		log.Fatal(err.Error())
	}
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))

	options := grpc_server.CommonGRPCServerOptions(env)
	server := grpc.NewServer(options...)
	reflection.Register(server)

	grpc_prometheus.Register(server)
	grpc_prometheus.EnableHandlingTimeHistogram()

	csspb.RegisterCodesearchServiceServer(server, css)
	ksspb.RegisterXRefServiceServer(server, css.XrefsService())
	ksspb.RegisterGraphServiceServer(server, css.GraphService())
	ksspb.RegisterFileTreeServiceServer(server, css.FiletreeService())
	ksspb.RegisterIdentifierServiceServer(server, css.IdentifierService())

	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(server))
	go func() {
		_ = server.Serve(lis)
	}()
	log.Printf("Codesearch server listening at %v", lis.Addr())
	env.GetHealthChecker().WaitForGracefulShutdown()
}
